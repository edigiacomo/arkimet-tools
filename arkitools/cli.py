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


def do_which_datasets(parser, args):
    from arkitools.dataset import which_datasets

    for ds in which_datasets(infiles=args.infile, dsconf=args.conf):
        print(ds["path"])


def do_merge_data(parser, args):
    from arkitools.merge import (
        merge_data, simple_merger, Vm2FlagsMerger,
        ReportMergedWriter, ImportWriter,
    )
    merger = {
        "simple": simple_merger,
        "vm2flags": Vm2FlagsMerger("all"),
        "vm2flags-B33196": Vm2FlagsMerger("B33196"),
    }.get(args.merger_type)

    writer = {
        "report": ReportMergedWriter(args.outfile, args.to_delete_file),
        "import": ImportWriter(),
    }.get(args.writer_type)

    merge_data(infiles=args.infile, dsconf=args.conf,
               merger=merger, writer=writer)


def do_repack_archived_file(parser, args):
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

    subparsers = parser.add_subparsers(title="command", dest="command",
                                       help="command to execute")
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
    merge_data_p = subparsers.add_parser('merge-data', description="Merge data")
    merge_data_p.add_argument("-m", "--merger-type",
                              choices=["simple", "vm2flags",
                                       "vm2flags-B33196"],
                              default="simple")
    merge_data_p.add_argument("-w", "--writer-type",
                              choices=["report", "import"],
                              default="report")
    merge_data_p.add_argument("-d", "--to-delete-file",
                              help="Save list of files to delete")
    merge_data_p.add_argument('-o', '--outfile', help="Merged data file")
    merge_data_p.add_argument('conf', help="Arkimet config file")
    merge_data_p.add_argument('infile', help="Input file", nargs="+")
    merge_data_p.set_defaults(func=do_merge_data)

    args = parser.parse_args()
    args.func(parser, args)


if __name__ == '__main__':
    main()
