# -*- coding: utf-8 -*-
"""Script to run sqlcmd commands to export tables"""
from collections import OrderedDict
from datetime import datetime
from imio.pyutils.system import runCommand

import argparse
import logging
import os
import re
import shutil


logging.basicConfig()
logger = logging.getLogger("sqlcmd")
logger.setLevel(logging.INFO)
sqlcmd_ext = ".fwf"
tables = OrderedDict(
    [
        ("XeAdresses", {}),
        ("eClassement", {}),
        ("eClassementDossiers", {}),
        ("eContacts", {}),
        ("eContactsAdresses", {}),
        ("eContactsTitres", {}),
        ("eContactsType", {}),
        # (
        #     "eCourriers",
        #     {
        #         "c": "isnull(Supprime, '0') != '1'",
        #         "o": "isnull(DateEncodage, dateentree), dateentree",
        #     },
        # ),
        ('eCourriers', {'c': "TypeEntrantSortant in ('E', 'S') and isnull(Supprime, '0') != '1'",
                        'o': "isnull(DateEncodage, dateentree), dateentree"}),
        # ('eCourriers', {'c': "TypeEntrantSortant in ('E', 'S') and isnull(Supprime, '0') != '1' and (dateentree != 0 "
        #                      "and DateEntree  > 20100100 or (dateencodage is not NULL and dateencodage > 20100100))",
        #                 'o': "isnull(DateEncodage, dateentree), dateentree"}),
        # ('eCourriers', {'c': "(TypeEntrantSortant != 'I' or isnull(NatureID, '') not in (select id from eNatures "
        #                      "where description like 'Délibération%' or description like 'Séance%') ) and "
        #                      "isnull(Supprime, '0') != '1'",
        #                 'o': "isnull(DateEncodage, dateentree), dateentree"}),
        ("eCourriersDestinataires", {}),
        ("eCourriersFichiers", {"o": "CourrierID, OrdreAffichage, DateUpload"}),
        # when no addresses view
        # ("eCourriersFichiers", {"o": "CourrierID, DateUpload"}),
        ("eCourriersDossiers", {"o": "CourrierID, Principal desc"}),
        ("eCourriersLiens", {}),
        ("eCourriersServices", {"o": "CourrierID, Principal desc"}),
        # ('eGroupes', {}), ('eGroupesContacts', {}), ('eGroupesMembres', {}),
        ("eNatures", {}),
        ("XeRues", {}),
        ("eServices", {}),
        ("eTypeAction", {}),
        ("eUsers", {}),
        ("eUsersServices", {}),
        ("vwContactsAdresses", {"o": "Id"}),
    ]
)
fwf_cmd = (
    'docker exec -u root -it {dock} /opt/mssql-tools/bin/sqlcmd -S localhost -d {db} -U SA -P "{pwd}" '
    '-Q "select * from {table}{where}{order}" -o "/srv/sqlcmd.fwf" -s"{sep}"'
)
cp_cmd = 'docker cp {dock}:/srv/sqlcmd.fwf "{of}"'


def main(docker, db_name, pwd, delim, input_filter, output_dir, only_new, simulate):
    start = datetime.now()
    logger.info("Start: {}".format(start.strftime("%Y%m%d-%H%M")))
    for table in tables:
        if table.startswith("X"):
            continue
        if input_filter and not re.match(input_filter, table):
            continue
        out_file = os.path.join(output_dir, "{}{}".format(table, sqlcmd_ext))
        if only_new and os.path.exists(out_file):
            continue
        where = ""
        if tables[table].get("c"):
            where = " where {}".format(tables[table]["c"])
        order = ""
        if tables[table].get("o"):
            order = " order by {}".format(tables[table]["o"])
        cmd = fwf_cmd.format(
            **{"dock": docker, "db": db_name, "pwd": pwd, "table": table, "sep": delim, "where": where, "order": order}
        )
        if simulate:
            logger.info(cmd)
            continue
        else:
            logger.info("ON sql table or view '{}'".format(table))
        logger.debug("cmdsql='{}'".format(cmd))
        (out, err, code) = runCommand(cmd)
        if code or err:
            logger.error("Problem in command '{}': {}".format(cmd, err))
            continue
        tmp_file = "/tmp/sqlcmd.fwf"
        cmd = cp_cmd.format(**{"dock": docker, "of": tmp_file})
        logger.debug("cp cmd='{}'".format(cmd))
        (out, err, code) = runCommand(cmd)
        if code or err:
            logger.error("Problem in command '{}': {}".format(cmd, err))
            continue
        shutil.move(tmp_file, out_file)
    logger.info("Script duration: %s" % (datetime.now() - start))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run sqlcmd files to export tables.")
    parser.add_argument("docker", help="Docker name.")
    parser.add_argument("password", help="Database password.")
    parser.add_argument("-d", "--database", dest="database", help="Database name.")
    parser.add_argument("-if", "--input_filter", dest="input_filter", help="Input filter.")
    parser.add_argument("-os", "--output_sep", dest="output_sep", help='Output delimiter. Default "|"', default="|")
    parser.add_argument("-od", "--output_dir", dest="output_dir", help='Output directory. Default "."', default=".")
    parser.add_argument(
        "-on", "--only_new", dest="only_new", action="store_true", help="Export only not existing fwf files."
    )
    parser.add_argument("-s", "--simulate", dest="simulate", action="store_true", help="Simulate operation")
    ns = parser.parse_args()
    if not ns.database:
        ns.database = ns.docker
    main(ns.docker, ns.database, ns.password, ns.output_sep, ns.input_filter, ns.output_dir, ns.only_new, ns.simulate)
