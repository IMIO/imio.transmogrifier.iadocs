# -*- coding: utf-8 -*-
"""Script to run sqlcmd commands to export tables"""
from datetime import datetime
from imio.pyutils.system import runCommand
from imio.pyutils.system import stop

import argparse
import logging
import os
import re


logging.basicConfig()
logger = logging.getLogger('sqlcmd')
logger.setLevel(logging.INFO)
sqlcmd_ext = '.fwf'
tables = ['eNatures', 'eServices']
fwf_cmd = 'docker exec -u root -it {dock} /opt/mssql-tools/bin/sqlcmd -S localhost -d {db} -U SA -P "{pwd}" ' \
          '-Q "select * from {table}" -o "/srv/sqlcmd.fwf" -s"{sep}"'
cp_cmd = 'docker cp {dock}:/srv/sqlcmd.fwf "{of}"'


def main(docker, db_name, pwd, delim, input_filter, output_dir):
    start = datetime.now()
    logger.info("Start: {}".format(start.strftime('%Y%m%d-%H%M')))
    for table in tables:
        if input_filter and not re.match(input_filter, table):
            continue
        logger.info("ON sql table or view '{}'".format(table))
        out_file = os.path.join(output_dir, '{}{}'.format(table, sqlcmd_ext))
        cmd = fwf_cmd.format(**{'dock': docker, 'db': db_name, 'pwd': pwd, 'table': table, 'sep': delim})
        logger.debug("cmdsql='{}'".format(cmd))
        (out, err, code) = runCommand(cmd)
        if code or err:
            logger.error("Problem in command '{}': {}".format(cmd, err))
            continue
        cmd = cp_cmd.format(**{'dock': docker, 'of': out_file})
        logger.debug("cp cmd='{}'".format(cmd))
        (out, err, code) = runCommand(cmd)
        if code or err:
            logger.error("Problem in command '{}': {}".format(cmd, err))
            continue
    logger.info("Script duration: %s" % (datetime.now() - start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run sqlcmd files to export tables.')
    parser.add_argument('docker', help='Docker name.')
    parser.add_argument('password', help='Database password.')
    parser.add_argument('-d', '--database', dest='database', help='Database name.')
    parser.add_argument('-if', '--input_filter', dest='input_filter', help='Input filter.')
    parser.add_argument('-os', '--output_sep', dest='output_sep', help='Output delimiter.', default='|')
    parser.add_argument('-od', '--output_dir', dest='output_dir', help='Output directory.', default='.')
    ns = parser.parse_args()
    if not ns.database:
        ns.database = ns.docker
    main(ns.docker, ns.database, ns.password, ns.output_sep, ns.input_filter, ns.output_dir)
