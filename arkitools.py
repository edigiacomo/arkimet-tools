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
    shutil.copyfile(os.path.join(src_ds, "config"), os.path.join(dst_ds, "config"))


def create_dataset(ds, ds_type="error", ds_step="daily"):
    """Create a dataset. Useful for create error or duplicates dataset."""
    os.makedirs(ds)
    with open(os.path.join(ds, "config"), "w") as out:
        out.write("type = {}\n".format(ds_type))
        out.write("step = {}\n".format(ds_step))


def repack_archived_file(infile, backup_file=None, dry_run=False):
    """Repack an archived file."""
    from subprocess import check_call, DEVNULL
    from glob import glob

    with tempfile.TemporaryDirectory() as tmpdir:
        src_ds = os.path.abspath(os.path.join(os.path.dirname(infile), "..", "..", ".."))
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
                    "--summary-restrict=reftime", infile])
        check_call(["arki-check", "-f", dst_ds])
        check_call(["arki-check", "-f", "-r", dst_ds])
        pattern = "/".join(os.path.normpath(os.path.abspath(infile)).split(os.sep)[-2:])
        f = glob("{}/{}".format(dst_ds, pattern)) + glob("{}/.archive/last/{}".format(dst_ds, pattern))
        if len(f) != 1:
            raise Exception("Expected one file archived, found {}: {}".format(len(f), ",".join(f)))
        outfile = f[0]
        if dry_run is True:
            print("Would copy {} to {}".format(outfile, infile))
        else:
            if backup_file:
                shutil.copyfile(infile, backup_file)

            shutil.copyfile(outfile, infile)


def which_datasets(infiles, dsconf):
    """Given a mergeconf, return the dataset paths that would acquire the
    files."""
    from configparser import ConfigParser
    from subprocess import check_output, DEVNULL
    cfg = ConfigParser()
    cfg.read([dsconf])
    for s in cfg.sections():
        if not "filter" in cfg[s] or not "path" in cfg[s]:
            continue
        p = cfg.get(s, "path")
        f = cfg.get(s, "filter")
        r = check_output(["arki-query", "--summary", "--dump", f] + infiles,
                         stderr=DEVNULL)
        if r and not r.isspace():
            yield p


def match_timeinterval_archivedfile(path, begin, end):
    """Check if file is within timeinterval.

    TODO: needs optimization.
    """
    from subprocess import check_output, DEVNULL
    q = "reftime:>={},<={}".format(begin.isoformat(), end.isoformat())
    r = check_output(["arki-query", "--summary", "--dump", q, path],
                     stderr=DEVNULL)
    if r and not r.isspace():
        return True
    else:
        return False


def overwrite_archived_data(infiles, dsconf):
    import json
    from datetime import datetime
    from glob import glob
    from subprocess import check_call, DEVNULL

    # Involved datasets
    datasets = set(which_datasets(infiles, dsconf))
    # Check time interval
    summ = json.loads(subprocess.check_output([
        "arki-query", "--summary", "--summary-restrict=reftime",
        "--json", ""] + infiles,
        stderr=DEVNULL))
    if len(summ["items"]) == 0:
        return
    b = datetime(*summ["items"][0]["summarystats"]["b"])
    e = datetime(*summ["items"][0]["summarystats"]["e"])
    # List of archived files involved
    originals = [
        f for ds in datasets for f in [
            f for f in glob("{}/.archive/*/*/*.*".format(ds))
            if not f.endswith(".metadata") or not f.endswith(".summary")
        ]
        if match_timeinterval_archivedfile(f, b, e)
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        dsdir = os.path.join(tmpdir, "datasets")
        cloned_datasets = []
        # Create work environment
        for ds in datasets:
            cloned_ds = os.path.join(dsdir, os.path.basename(ds))
            clone_dataset(ds, cloned_ds)
            cloned_datasets.append(cloned_ds)

        # Add error and duplicates
        create_dataset(os.path.join(dsdir, "error"), "error")
        create_dataset(os.path.join(dsdir, "duplicates"), "duplicates")
        # Create config file
        with open(os.path.join(tmpdir, "conf"), "w") as fp:
            check_call(["arki-mergeconf"] + glob("{}/*".format(dsdir)),
                       stdout=fp)

    raise Exception("Not yet implemented!")


def do_clone_dataset(args):
    return clone_dataset(src_ds=args.srcds, dst_ds=args.dstds)


def do_repack_archived_file(args):
    return repack_archived_file(
        infile=args.infile,
        backup_file=args.backup_file,
        dry_run=args.dry_run
    )


def do_which_datasets(args):
    for ds in which_datasets(infiles=args.infile, dsconf=args.conf):
        print(ds)


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(description='Arkimet tools')
    parser.add_argument("--version", action="version", version="%(prog)s 0.1")

    subparsers = parser.add_subparsers(
        title="command", metavar="COMMAND",
        dest="command", help='sub-command help')
    subparsers.required = True
    # Clone dataset
    clone_dataset_p = subparsers.add_parser('clone-dataset', description='Clone a dataset (without data)')
    clone_dataset_p.add_argument('srcds', help='Source dataset')
    clone_dataset_p.add_argument('dstds', help='Destination dataset')

    # Repack archived file
    repack_archived_file_p = subparsers.add_parser('repack-archived-file', description="Repack and archived file")
    repack_archived_file_p.add_argument("-n", "--dry-run", action="store_true", help="Dry run")
    repack_archived_file_p.add_argument("-b", "--backup-file", help="Save original data")
    repack_archived_file_p.add_argument("infile", help="File to repack")
    repack_archived_file_p.set_defaults(func=do_repack_archived_file)

    # Which dataset
    which_dataset_p = subparsers.add_parser('which-datasets', description="List dataset paths that would acquire the file")
    which_dataset_p.add_argument('conf', help="Config file about input sources")
    which_dataset_p.add_argument('infile', help="File to inspect", nargs="+")
    which_dataset_p.set_defaults(func=do_which_datasets)

    args = parser.parse_args()
    args.func(args)
