# arkitools/merge - merge utils
#
# Copyright (C) 2015  - ARPA-SIMC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Author: Emanuele Di Giacomo <edigiacomo@arpa.emr.it>


def merge_data(infiles, dsconf, merger, writer):
    """Create a merge from infiles and archived data involved.

    - infiles: list of new files to merge
    - dsconf: datasets involved
    - merger: policy for mergeing (if None, overwrite).
    - writer: policy for writing the results (if None, do nothing).

    The merger merge the old and new data in a temporary dataset.
    It is a callable with the following parameters:
    - old_data: list of old files involved in the merge
    - new_data: list of new files involved in the merge
    - old_dsconf: dsconf of the original datasets
    - new_dsconf: dsconf where the resulting data must be merged by the merger

    TODO: The writer write the merged data. It is a callable with the following
    parameters:
    - old_data: list of old files involved in the merge
    - new_data: list of new files involved in the merge
    - old_dsconf: dsconf of the original datasets
    - new_dsconf: dsconf where the resulting data are merged
    """
    import os
    import json
    import tempfile
    from datetime import datetime
    from glob import glob
    from subprocess import check_call, check_output, DEVNULL
    from .dataset import (
        which_datasets, is_file_within_timeinterval, create_dataset,
        clone_dataset,
    )

    # Involved datasets
    datasets = list(which_datasets(infiles, dsconf))
    # Check time interval
    summ = json.loads(check_output([
        "arki-query", "--summary", "--summary-restrict=reftime",
        "--json", ""] + infiles).decode("utf-8"))
    if len(summ["items"]) == 0:
        return []
    b = datetime(*summ["items"][0]["summarystats"]["b"])
    e = datetime(*summ["items"][0]["summarystats"]["e"])
    # List of archived files involved
    originals = [
        f for ds in datasets for f in [
            f for f in glob("{}/.archive/*/*/*.*".format(ds["path"]))
            if not f.endswith(".metadata") and not f.endswith(".summary")
        ]
        if is_file_within_timeinterval(f, b, e, archived=True, step=ds["step"])
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        dsdir = os.path.join(tmpdir, "datasets")
        cloned_datasets = []
        # Create work environment
        for ds in datasets:
            cloned_ds = os.path.join(dsdir, os.path.basename(ds["path"]))
            clone_dataset(ds["path"], cloned_ds)
            cloned_datasets.append(cloned_ds)

        # Add error and duplicates
        err_ds = os.path.join(dsdir, "error")
        dup_ds = os.path.join(dsdir, "duplicates")
        create_dataset(err_ds, "error")
        create_dataset(dup_ds, "duplicates")
        # Create config file
        config = os.path.join(tmpdir, "conf")
        with open(config, "w") as fp:
            check_call(["arki-mergeconf", err_ds, dup_ds] + cloned_datasets,
                       stdout=fp)
        # merge data
        merger(old_data=originals, new_data=infiles, old_dsconf=dsconf,
               new_dsconf=config)
        # arki-check
        check_call(["arki-check", "-f"] + cloned_datasets, stdout=DEVNULL)
        check_call(["arki-check", "-f", "-r"] + cloned_datasets,
                   stdout=DEVNULL)
        # write data
        writer(old_data=originals, new_data=infiles, old_dsconf=dsconf,
               new_dsconf=config)
        return originals


def simple_merger(old_data, new_data, old_dsconf, new_dsconf):
    """Merger for merge_data."""
    from subprocess import check_call, DEVNULL
    # Import old data
    if old_data:
        check_call(["arki-scan", "--dispatch="+new_dsconf, "--dump",
                    "--summary", "--summary-restrict=reftime"] + old_data,
                   stdout=DEVNULL)

    # Import new data
    check_call(["arki-scan", "--dispatch="+new_dsconf, "--dump", "--summary",
                "--summary-restrict=reftime"] + new_data, stdout=DEVNULL)


class Vm2FlagsMerger(object):
    def __init__(self, flags="all"):
        self.flags_sql = {
            "all": "?",
            "B33196": "substr(?, 1, 1) || substr(f, 2)"
        }[flags]

    def __call__(self, old_data, new_data, old_dsconf, new_dsconf):
        """Merge flags for VM2 data."""
        import sqlite3
        from tempfile import NamedTemporaryFile
        import csv
        from subprocess import check_call, DEVNULL
        with NamedTemporaryFile() as dbfp:
            db = sqlite3.connect(dbfp.name)
            db.execute((
                "CREATE TABLE vm2 "
                "(d varchar, s varchar, v varchar, "
                " v1 varchar, v2 varchar, v3 varchar, "
                " f varchar);"
            ))
            db.commit()
            for f in old_data:
                with open(f) as fp:
                    reader = csv.reader(fp)
                    for row in reader:
                        row[0] = row[0][0:14]
                        db.execute(
                            "INSERT INTO vm2 VALUES (?, ?, ?, ?, ?, ?, ?)",
                            row
                        )

                db.commit()

            for f in new_data:
                with open(f) as fp:
                    reader = csv.reader(fp)
                    for row in reader:
                        row[0] = row[0][0:14]

                        db.execute((
                            "UPDATE vm2 SET f = {}"
                            "WHERE d = ? AND s = ? AND v = ?"
                        ).format(self.flags_sql), (
                            row[6], row[0], row[1], row[2]
                        ))

                db.commit()

            cur = db.cursor()
            cur.execute("SELECT * FROM vm2")
            with NamedTemporaryFile("w", suffix=".vm2") as outfp:
                for row in cur:
                    outfp.write(",".join(row) + "\n")

                outfp.flush()
                check_call(["arki-scan", "--dispatch="+new_dsconf, "--dump",
                            outfp.name], stdout=DEVNULL)


class ReportMergedWriter(object):
    """Writer for merge_data.

    Save the merged data in outfile and list the archived file to delete."""
    def __init__(self, outfile, todelete):
        self.outfile = outfile
        self.todelete = todelete

    def __call__(self, old_data, new_data, old_dsconf, new_dsconf):
        from subprocess import check_call, DEVNULL
        # Save new data in outfile
        check_call(["arki-query", "--data", "-C", new_dsconf, "-o",
                    self.outfile, ""], stdout=DEVNULL)
        with open(self.todelete, "w") as fp:
            for f in old_data:
                fp.write(f + "\n")
