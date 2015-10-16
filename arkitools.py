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


def do_clone_dataset(args):
    return clone_dataset(src_ds=args.srcds, dst_ds=args.dstds)


def do_repack_archived_file(args):
    return repack_archived_file(
        infile=args.infile,
        backup_file=args.backup_file,
        dry_run=args.dry_run
    )


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

    args = parser.parse_args()
    args.func(args)