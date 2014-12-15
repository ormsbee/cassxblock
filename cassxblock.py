"""
Import records from StudentModuleHistory to Cassandra.
"""
from __future__ import absolute_import, division, print_function
from builtins import object, int

from collections import namedtuple
import logging
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import click
import pymysql
import pymysql.cursors

log = logging.getLogger(__name__)

@click.group()
@click.option("--cass_keyspace", default="xblock_import_ks", help="Cassandara keyspace")
@click.option("--cass_hosts", default="127.0.0.1", help="Comma separated list of Cassandra hosts")
@click.option("--cass_port", default=9042, help="Cassandra port")
@click.option("--cass_user", default="cassandra", help="Cassandra user")
@click.option("--cass_password", default="cassandra", help="Cassandra password")
@click.pass_context
def cli(ctx, cass_keyspace, cass_hosts, cass_port, cass_user, cass_password):
    """Script to test loading student state from edx-platform to Cassandra.

    Currently, we're just transferring over entries from StudentModuleHistory
    (the courseware_studentmodulehistory table). This script does database
    access directly, and does not use Django.

    While this script takes a lot of options, everything is defaulted to what
    you'd get in devstack and new install of Cassandra.

    This will create a keyspace if it does not already exist, but it won't do
    so in any smart sort of way (because I don't know how they should be
    configured). So if you're going to do something smart there, create the
    keyspace in advance and just point this script to it.
    """
    # Cassandra Session init
    cluster = Cluster(
        contact_points=[host.strip() for host in cass_hosts.split(",")],
        port=cass_port,
        auth_provider=PlainTextAuthProvider(
            username=cass_user, password=cass_password
        ),
        compression=True,
        protocol_version=3
    )

    # Force the creation of the keyspace
    session = cluster.connect()
    session.execute(
        """CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = 
           {{'class' : 'SimpleStrategy', 'replication_factor' : 3 }};"""
        .format(cass_keyspace)
    )

    # Switch the session to use the specified keyspace, and send it to our
    # sub-commands
    session.set_keyspace(cass_keyspace)
    ctx.obj['cass_session'] = session

    click.echo(
        "Connected to Cassandra cluster: {} (keyspace: {})"
        .format(cluster.metadata.cluster_name, session.keyspace)
    )


@cli.command()
@click.pass_context
def setup_cassandra(ctx):
    """Create the keyspace and table where we'll store data in Cassandra."""
    session = ctx.obj['cass_session']
    session.execute("DROP TABLE IF EXISTS xblock_user_state;")
    session.execute("""
        CREATE TABLE IF NOT EXISTS xblock_user_state (
            user_id int,
            course_key varchar,
            block_type varchar,
            usage_key varchar,
            created timestamp,
            save_id timeuuid,
            version varchar,
            state text,
            PRIMARY KEY ((user_id, course_key), block_type, usage_key, created, save_id)
        )
        WITH
            CLUSTERING ORDER BY (block_type ASC, usage_key ASC, created DESC, save_id DESC) AND 
            COMPRESSION = {
                'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'
            };
    """)
    click.echo("Created table xblock_user_state in keyspace {}".format(session.keyspace))


@cli.command()
@click.option("--mysql_host", default="127.0.0.1", help="MySQL host")
@click.option("--mysql_port", default=3306, help="MySQL port")
@click.option("--mysql_db", default="edxapp", help="MySQL database name")
@click.option("--mysql_user", default="edxapp001", help="MySQL user")
@click.option("--mysql_password", default="", help="MySQL password")
@click.option("--start_record", default=1, help="Record ID to start from")
@click.option("--num_records", default=0, help="Number of records to migrate (use 0 for no limit)")
@click.pass_context
def import_records(ctx, mysql_host, mysql_port, mysql_db, mysql_user,
                   mysql_password, start_record, num_records):
    """Import student state from courseware_studentmodulehistory into Cassandra."""
    connection = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
    )
    cursor = connection.cursor(pymysql.cursors.SSDictCursor)

    # Get the ID of the last entry in courseware_studentmodulehistory
    cursor.execute("select id from courseware_studentmodulehistory order by id desc limit 1")
    row = cursor.fetchone()
    max_row_id = row['id']

    cursor.execute("""
        select
            csm.student_id, csm.course_id, csm.module_type, csm.module_id,
            h.id, h.created, h.state
        from
            courseware_studentmodule csm, courseware_studentmodulehistory h
        where 
            h.student_module_id=csm.id and
            h.id >= %s
        order by h.id
        """,
        [start_record]
    )

    # Prepare our Cassandra writer
    cass_session = ctx.obj['cass_session']
    insert_statement = cass_session.prepare("""
        INSERT INTO xblock_user_state
            (user_id, course_key, block_type, usage_key, created, save_id, version, state)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?);
    """)

    # Now start actually writing our records
    click.echo("Start\t\tMax\t\tWritten")
    num_written = 0

    while True:
        if num_records and num_written >= num_records:
            break

        rows = cursor.fetchmany()
        if not rows:
            break

        for row in rows:
            # TODO: If we can make the uuid1 generated for the save_id be a
            # function of the row_id and the timestamp, we can do this import
            # repeatedly without creating extra records. The main reason we want
            # this save_id in the first place is to a) have a way to differentiate
            # two states that have the same timestamp (old MySQL only tracks to
            # seconds granularity); and b) To have a way to reference historical
            # states in other things (e.g. scores).
            cass_session.execute(
                insert_statement,
                (
                    row['student_id'], row['course_id'], row['module_type'],
                    row['module_id'], row['created'], uuid.uuid1(), None, row['state']
                )
            )
            num_written += 1
            if num_records and num_written >= num_records:
                break

        click.echo("\r{}\t\t{}\t\t{}".format(start_record, max_row_id, num_written), nl=False)

    click.echo("\nWrote {} records, last ID written was {}".format(num_written, row['id']))
    cursor.close()
    connection.close()



if __name__ == '__main__':
    cli(obj={})
