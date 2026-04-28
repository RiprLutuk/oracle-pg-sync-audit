import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.reports.writer_html import write_html_report


class WriterHtmlTest(unittest.TestCase):
    def test_run_report_links_local_manifest_and_workbook(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run_20260429_021021_adfa95e5d60d"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text("{}", encoding="utf-8")
            (run_dir / "report.xlsx").write_text("", encoding="utf-8")
            path = run_dir / "report.html"

            write_html_report(path, inventory_rows=[], column_diff_rows=[])

            html = path.read_text(encoding="utf-8")

        self.assertIn('href="manifest.json"', html)
        self.assertIn('href="report.xlsx"', html)
        self.assertNotIn('href="run_', html)


if __name__ == "__main__":
    unittest.main()
