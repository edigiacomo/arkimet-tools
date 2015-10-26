# arkitools/scripts - scripts
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


def do_which_datasets(args):
    from arkitools.dataset import which_datasets

    for ds in which_datasets(infiles=args.infile, dsconf=args.conf):
        print(ds["path"])


def do_report_merged_data(args):
    from arkitools.merge import (
        merge_data, simple_merger, vm2_flags_merger,
        ReportMergedWriter,
    )

    merger = {
        "simple": simple_merger,
        "vm2flags": vm2_flags_merger,
    }.get(args.merger_type)

    merge_data(infiles=args.infile, dsconf=args.conf,
               merger=merger,
               writer=ReportMergedWriter(args.outfile, args.to_delete_file))


def do_repack_archived_file(args):
    from arkitools.dataset import repack_archived_file

    return repack_archived_file(
        infile=args.infile,
        backup_file=args.backup_file,
        dry_run=args.dry_run,
        tmpbasedir=args.tmpdir
    )


def main():
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

    # Report merge data
    report_merged_data_p = subparsers.add_parser(
        'report-merge-data',
        description=(
            "Create a file with merged data and print a list of files "
            "to delete"
        )
    )
    report_merged_data_p.add_argument("-m", "--merger-type",
                                      choices=["simple", "vm2flags"],
                                      default="simple")
    report_merged_data_p.add_argument("-d", "--to-delete-file", required=True,
                                      help="Save list of files to delete")
    report_merged_data_p.add_argument('-o', '--outfile', required=True,
                                      help="Merged data file")
    report_merged_data_p.add_argument('conf', help="Arkimet config file")
    report_merged_data_p.add_argument('infile', help="Input file", nargs="+")
    report_merged_data_p.set_defaults(func=do_report_merged_data)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
