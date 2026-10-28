"""Standalone scripts tests (scripts folder is not a package: the scripts are run by path)."""

from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING
from plone.base.utils import safe_text
from zope.testing.loggingsupport import InstalledHandler

import codecs
import imio.transmogrifier.iadocs
import os
import shutil
import string
import subprocess
import sys
import tempfile
import unittest


SCRIPTS = os.path.join(os.path.dirname(imio.transmogrifier.iadocs.__file__), "scripts")
# sqlcmd output: header, dashes, fixed width rows separated by |, empty line, rows count
FWF = (
    "Id|Name      |Amount\n"
    "--|----------|------\n"
    " 1|Été       |    12\n"
    " 2|Bob       |  -345\n"
    "\n"
    "(2 rows affected)\n"
)
CSV = b'"Id","Name","Amount"\n1,"\xc3\x89t\xc3\xa9",12\n2,"Bob",-345\n'
SQLCMD_TABLES = [
    "eClassement",
    "eClassementDossiers",
    "eContacts",
    "eContactsAdresses",
    "eContactsTitres",
    "eContactsType",
    "eCourriers",
    "eCourriersDestinataires",
    "eCourriersFichiers",
    "eCourriersDossiers",
    "eCourriersLiens",
    "eCourriersServices",
    "eNatures",
    "eServices",
    "eTypeAction",
    "eUsers",
    "eUsersServices",
    "vwContactsAdresses",
]


def load_script(path, **globs):
    """Executes a script body (not as __main__) and returns its namespace (kept alive with its functions)."""
    namespace = dict(globs, __name__=os.path.basename(path)[:-3], __file__=path)
    with open(path, "rb") as fh:
        exec(compile(fh.read(), path, "exec"), namespace)
    return namespace


class TestScripts(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.tmp = safe_text(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def path(self, filename):
        return os.path.join(self.tmp, filename)

    def write(self, filename, content):
        with open(self.path(filename), "wb") as fh:
            fh.write(content)

    def read(self, filename):
        with open(self.path(filename), "rb") as fh:
            return fh.read()

    def test_generate_csv_headers(self):
        script = os.path.join(SCRIPTS, "generate_csv_headers.py")
        self.assertEqual(subprocess.check_output([sys.executable, script, "C"]), b"_A _B _C\n")
        letters = list(string.ascii_uppercase)
        out = subprocess.check_output([sys.executable, script, "AB", "-p", "x", "-s", ","])
        self.assertEqual(out, (",".join(["x" + c for c in letters + ["AA", "AB"]]) + "\n").encode("ascii"))
        # unknown last one: all headers until ZZ
        out = subprocess.check_output([sys.executable, script, "?"]).split()
        self.assertEqual((len(out), out[0], out[-1]), (26 * 27, b"_A", b"_ZZ"))

    def test_fwf_to_csv(self):
        script = load_script(os.path.join(SCRIPTS, "fwf_to_csv.py"))
        logs = InstalledHandler("csv")
        self.addCleanup(logs.uninstall)
        self.write("t1.fwf", FWF.encode("utf8"))
        # get_records_info: rows count and position, file pointer reset
        with codecs.open(self.path("t1.fwf"), "r", encoding="utf8") as fh:
            self.assertEqual(script["get_records_info"](fh, "\n"), (2, len(FWF.encode("utf8")) - 20))
            self.assertEqual(fh.tell(), 0)
            # get_cols: columns widths, pointer after the dashes line
            cols = script["get_cols"](fh, "|", "\n")
            self.assertEqual(list(cols.items()), [("Id", 2), ("Name", 10), ("Amount", 6)])
            # get_values: numbers (right aligned) converted, texts stripped (text, written by a text csv writer)
            counters = {"read": 0, "max": 2}
            self.assertEqual(script["get_values"](cols, fh, counters, "|"), (True, [1, "Été", 12]))
            self.assertEqual(script["get_values"](cols, fh, counters, "|"), (True, [2, "Bob", -345]))
            self.assertEqual(script["get_values"](cols, fh, counters, "|"), (False, []))
            self.assertEqual(counters, {"read": 2, "max": 2})
        # main: only .fwf files, output in other dir
        self.write("t2.fwf", FWF.replace("Bob", "Bil").encode("utf8"))
        self.write("other.txt", b"x")
        os.mkdir(self.path("out"))
        script["main"](self.tmp, self.path("out"), False, None, "|", False, None, "lf")
        self.assertEqual(sorted(os.listdir(self.path("out"))), ["t1.csv", "t2.csv"])
        self.assertEqual(self.read("out/t1.csv"), CSV)
        self.assertEqual(self.read("out/t2.csv"), CSV.replace(b"Bob", b"Bil"))
        # counter column, input filter, only new
        self.write("out/t2.csv", b"old")
        script["main"](self.tmp, self.path("out"), True, r"t\d", "|", True, None, "lf")
        self.assertEqual(self.read("out/t2.csv"), b"old")
        script["main"](self.tmp, self.path("out"), True, r"t1", "|", False, None, "lf")
        self.assertEqual(
            self.read("out/t1.csv"), b'"Line","Id","Name","Amount"\n1,1,"\xc3\x89t\xc3\xa9",12\n2,2,"Bob",-345\n'
        )
        self.assertEqual(self.read("out/t2.csv"), b"old")
        # crlf, french rows count line, other separator
        for filename in os.listdir(self.tmp):
            os.path.isfile(self.path(filename)) and os.remove(self.path(filename))
        fwf = FWF.replace("|", ";").replace("(2 rows affected)", "(2 lignes affectées)").replace("\n", "\r\n")
        self.write("t3.fwf", fwf.encode("utf8"))
        script["main"](self.tmp, self.tmp, False, None, ";", False, None, "crlf")
        self.assertEqual(self.read("t3.csv"), CSV)
        # iconv: the original file is renamed and converted (needs the iconv command, run in a temp dir)
        os.remove(self.path("t3.fwf"))
        self.write("t4.fwf", FWF.encode("latin1"))
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp)
        script["main"](self.tmp, self.tmp, False, r"t4", "|", False, "latin1", "lf")
        self.assertEqual(self.read("t4.fwf.latin1"), FWF.encode("latin1"))
        self.assertEqual(self.read("t4.fwf"), FWF.encode("utf8"))
        self.assertEqual(self.read("t4.csv"), CSV)
        # wrong rows count
        logs.clear()
        self.write("t5.fwf", FWF.replace("(2 rows", "(3 rows").encode("utf8"))
        script["main"](self.tmp, self.tmp, False, r"t5", "|", False, None, "lf")
        self.assertEqual(self.read("t5.csv"), CSV)
        self.assertIn(
            ("ERROR", "We don't have the correct records number: writed 2, must have 3"),
            [(rec.levelname, rec.getMessage()) for rec in logs.records],
        )
        # stops: no rows count line, wrong dashes line
        self.write("t6.fwf", FWF.replace("Été", "Ete").replace("(2 rows affected)", "").encode("utf8"))
        self.assertRaises(SystemExit, script["main"], self.tmp, self.tmp, False, r"t6", "|", False, None, "lf")
        # Plone 4 behaviour (bug?): seeking backward by bytes in an utf8 file can split a character
        self.write("t8.fwf", FWF.replace("(2 rows affected)", "").encode("utf8"))
        self.assertRaises(UnicodeDecodeError, script["main"], self.tmp, self.tmp, False, r"t8", "|", False, None, "lf")
        self.write("t7.fwf", FWF.replace("--|", "-|-").encode("utf8"))
        self.assertRaises(SystemExit, script["main"], self.tmp, self.tmp, False, r"t7", "|", False, None, "lf")

    def test_mssqlcmd_to_fwf(self):
        script = load_script(os.path.join(SCRIPTS, "mssqlcmd_to_fwf.py"))
        logs = InstalledHandler("sqlcmd")
        self.addCleanup(logs.uninstall)
        # simulate: commands logged, X tables skipped
        script["main"]("dock", "db", "pwd", "|", None, self.tmp, False, True)
        messages = [rec.getMessage() for rec in logs.records]
        self.assertTrue(messages[0].startswith("Start: "))
        self.assertTrue(messages[-1].startswith("Script duration: "))
        self.assertEqual(len(messages), 2 + len(SQLCMD_TABLES))
        self.assertEqual([msg.split(" from ")[1].split(" ")[0].strip('"') for msg in messages[1:-1]], SQLCMD_TABLES)
        self.assertEqual(
            messages[1],
            'docker exec -u root -it dock /opt/mssql-tools/bin/sqlcmd -S localhost -d db -U SA -P "pwd" '
            '-Q "select * from eClassement" -o "/srv/sqlcmd.fwf" -s"|"',
        )
        self.assertEqual(
            messages[1 + SQLCMD_TABLES.index("eCourriers")],
            'docker exec -u root -it dock /opt/mssql-tools/bin/sqlcmd -S localhost -d db -U SA -P "pwd" '
            "-Q \"select * from eCourriers where TypeEntrantSortant in ('E', 'S') and isnull(Supprime, '0') != '1' "
            'order by case when isnull(dateentree, 0) != 0 then dateentree else isnull(dateencodage, 0) end" '
            '-o "/srv/sqlcmd.fwf" -s"|"',
        )
        # input filter, only new
        logs.clear()
        self.write("eCourriersLiens.fwf", b"")
        script["main"]("dock", "db", "pwd", ";", "eCourriers", self.tmp, True, True)
        self.assertEqual(
            [rec.getMessage().split(" from ")[1].split(" ")[0].strip('"') for rec in logs.records[1:-1]],
            ["eCourriers", "eCourriersDestinataires", "eCourriersFichiers", "eCourriersDossiers", "eCourriersServices"],
        )
        self.assertTrue(
            logs.records[3]
            .getMessage()
            .endswith(
                'from eCourriersFichiers order by CourrierID, OrdreAffichage, DateUpload" -o "/srv/sqlcmd.fwf" -s";"'
            )
        )
