#!/usr/bin/env python3

# arkitools - Tools for arkimet
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

import os
import shutil
import tempfile


def validate_dataset(ds):
    """Raise exception if the given path is not a valid dataset."""
    if not os.path.isdir(ds):
        raise Exception("Invalid dataset: {} is not a directory".format(ds))
    if not os.path.exists(os.path.join(ds, "config")):
        raise Exception("Invalid dataset: {}/config not found".format(ds))


def clone_dataset(src_ds, dst_ds):
    """Clone a dataset."""
    validate_dataset(src_ds)
    os.makedirs(dst_ds)
    shutil.copyfile(os.path.join(src_ds, "config"),
                    os.path.join(dst_ds, "config"))


def create_dataset(ds, ds_type="error", ds_step="daily"):
    """Create a dataset. Useful for create error or duplicates dataset."""
    os.makedirs(ds)
    with open(os.path.join(ds, "config"), "w") as out:
        out.write("type = {}\n".format(ds_type))
        out.write("step = {}\n".format(ds_step))


def repack_archived_file(infile, backup_file=None, dry_run=False, tmpbasedir=None):
    """Repack an archived file."""
    from subprocess import check_call, DEVNULL
    from glob import glob

    with tempfile.TemporaryDirectory(dir=tmpbasedir) as tmpdir:
        src_ds = os.path.abspath(os.path.join(os.path.dirname(infile),
                                              "..", "..", ".."))
        dst_ds = os.path.join(tmpdir, os.path.basename(src_ds))
        err_ds = os.path.join(tmpdir, "error")
        dup_ds = os.path.join(tmpdir, "duplicates")
        clone_dataset(src_ds, dst_ds)
        create_dataset(err_ds, "error", "daily")
        create_dataset(dup_ds, "duplicates", "daily")
        config = os.path.join(tmpdir, "conf")
        with open(config, "w") as fp:
            check_call(["arki-mergeconf", dst_ds, err_ds, dup_ds], stdout=fp)

        check_call(["arki-scan", "--dispatch="+config, "--dump", "--summary",
                    "--summary-restrict=reftime", infile], stdout=DEVNULL)
        check_call(["arki-check", "-f", dst_ds], stdout=DEVNULL)
        check_call(["arki-check", "-f", "-r", dst_ds], stdout=DEVNULL)
        pattern = "/".join(os.path.normpath(
            os.path.abspath(infile)
        ).split(os.sep)[-2:])
        f = glob("{}/{}".format(dst_ds, pattern))
        f.extend(glob("{}/.archive/last/{}".format(dst_ds, pattern)))
        if len(f) != 1:
            raise Exception(
                "Expected one file archived, found {}: {}".format(
                    len(f), ",".join(f)
                )
            )
        outfile = f[0]
        if dry_run is True:
            print("Would copy {} to {}".format(outfile, infile))
        else:
            if backup_file:
                shutil.copyfile(infile, backup_file)

            shutil.copyfile(outfile, infile)


def which_datasets(infiles, dsconf):
    """Given a mergeconf, return the datasets that would acquire the
    files as a dict."""
    from configparser import ConfigParser
    from subprocess import check_output, DEVNULL
    cfg = ConfigParser()
    cfg.read([dsconf])
    for s in cfg.keys():
        section = dict(cfg.items(s))
        if "filter" not in section or "path" not in section:
            continue
        f = section.get("filter")
        r = check_output(["arki-query", "--summary", "--dump", f] + infiles)
        if r and not r.isspace():
            yield section


def guess_step_from_path(path):
    import re
    if re.match('^.*/(\d{4})/(\d{2})-(\d{2}).*$', path):
        return "daily"
    else:
        return None


def archived_file_within_timeinterval(path, begin, end, step=None):
    # yearly: YY/YYYY
    # monthly: YYYY/mm
    # biweekly: YYYY/mm-{1,2}
    # weekly: YYYY/mm-{1,2,3,4,5}
    # daily: YYYY/mm-dd
    # singlefile: YYYY/mm/dd/HH
    import re
    from datetime import datetime, timedelta
    abspath = os.path.abspath(path)

    if step is None:
        step = guess_step_from_path(abspath)

    if step == "daily":
        g = re.match('^.*/(\d{4})/(\d{2})-(\d{2}).*$', abspath)
        b = datetime(*map(int, g.groups()))
        e = b + timedelta(days=1)
        return all([begin < e, b <= end])
    else:
        raise Exception("Cannot check timeinterval for {}".format(path))


def generic_file_within_timeinterval(path, begin, end):
    from subprocess import check_output, DEVNULL
    q = "reftime:>={},<={}".format(begin.isoformat(), end.isoformat())
    r = check_output(["arki-query", "--summary", "--dump", q, path])
    return r and not r.isspace()


def file_within_timeinterval(path, begin, end, archived=False, step=None):
    """Check if file is within timeinterval."""
    if archived:
        try:
            return archived_file_within_timeinterval(path, begin, end, step)
        except:
            return generic_file_within_timeinterval(path, begin, end)
    else:
        return generic_file_within_timeinterval(path, begin, end)


def naif_merger(old_data, new_data, old_dsconf, new_dsconf):
    """Merger for merge_data."""
    from subprocess import check_call, DEVNULL
    # Import old data
    if old_dsconf:
        check_call(["arki-scan", "--dispatch="+new_dsconf, "--dump",
                    "--summary", "--summary-restrict=reftime"] + old_dsconf,
                   stdout=DEVNULL)

    # Import new data
    check_call(["arki-scan", "--dispatch="+new_dsconf, "--dump", "--summary",
                "--summary-restrict=reftime"] + new_data, stdout=DEVNULL)


def merge_data(infiles, dsconf, merger, writer=None):
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

    The writer write the merged data. It is a callable with the following
    parameters:
    - old_data: list of old files involved in the merge
    - new_data: list of new files involved in the merge
    - old_dsconf: dsconf of the original datasets
    - new_dsconf: dsconf where the resulting data are merged
    """
    import json
    from datetime import datetime
    from glob import glob
    from subprocess import check_call, check_output, DEVNULL

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
        if file_within_timeinterval(f, b, e, archived=True, step=ds["step"])
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

        # arki-check
        check_call(["arki-check", "-f"] + cloned_datasets, stdout=DEVNULL)
        check_call(["arki-check", "-f", "-r"] + cloned_datasets, stdout=DEVNULL)
        # Save new data in outfile
        check_call(["arki-query", "--data", "-C", config, "-o",
                    outfile, ""], stdout=DEVNULL)
        return originals


def do_clone_dataset(args):
    return clone_dataset(src_ds=args.srcds, dst_ds=args.dstds)


def do_repack_archived_file(args):
    return repack_archived_file(
        infile=args.infile,
        backup_file=args.backup_file,
        dry_run=args.dry_run,
        tmpbasedir=args.tmpdir
    )


def do_which_datasets(args):
    for ds in which_datasets(infiles=args.infile, dsconf=args.conf):
        print(ds["path"])


def do_merge_data(args):
    with open(args.to_delete_file, "w") as fp:
        for f in merge_data(infiles=args.infile, dsconf=args.conf,
                            outfile=args.outfile, merger=naif_merger):
            fp.write(f + "\n")


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(description='Arkimet tools')
    parser.add_argument("--version", action="version", version="%(prog)s 0.1")

    subparsers = parser.add_subparsers(
        title="command", metavar="COMMAND",
        dest="command", help='sub-command help')
    subparsers.required = True
    # Clone dataset
    clone_dataset_p = subparsers.add_parser(
        'clone-dataset', description='Clone a dataset (without data)'
    )
    clone_dataset_p.add_argument('srcds', help='Source dataset')
    clone_dataset_p.add_argument('dstds', help='Destination dataset')

    # Repack archived file
    repack_archived_file_p = subparsers.add_parser(
        'repack-archived-file',
        description="Repack and archived file"
    )
    repack_archived_file_p.add_argument("-p", "--tmpdir",
                                        help="Temporary directory prefix")
    repack_archived_file_p.add_argument("-n", "--dry-run",
                                        action="store_true", help="Dry run")
    repack_archived_file_p.add_argument("-b", "--backup-file",
                                        help="Save original data")
    repack_archived_file_p.add_argument("infile", help="File to repack")
    repack_archived_file_p.set_defaults(func=do_repack_archived_file)

    # Which dataset
    which_dataset_p = subparsers.add_parser(
        'which-datasets',
        description="List dataset paths that would acquire the file"
    )
    which_dataset_p.add_argument('conf',
                                 help="Config file about input sources")
    which_dataset_p.add_argument('infile', help="File to inspect", nargs="+")
    which_dataset_p.set_defaults(func=do_which_datasets)

    # Merge data
    merge_data_p = subparsers.add_parser(
        'merge-data',
        description=(
            "Create a file with merged data and print a list of files "
            "to delete"
        )
    )
    merge_data_p.add_argument("-d", "--to-delete-file", required=True,
                              help="Save list of files to delete")
    merge_data_p.add_argument('-o', '--outfile', required=True,
                              help="Outfile where merged data are saved")
    merge_data_p.add_argument('conf', help="Arkimet config file")
    merge_data_p.add_argument('infile', help="File to import", nargs="+")
    merge_data_p.set_defaults(func=do_merge_data)

    args = parser.parse_args()
    args.func(args)
