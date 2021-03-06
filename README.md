# arkimet tools

Tools for [arkimet](https://github.com/ARPA-SIMC/arkimet).

## Install

Install the version `0.3` with pip:

    pip install git+https://github.com/edigiacomo/arkimet-tools@v0.3


## Clone an empty dataset

    arkitools-cli clone-dataset src dst

## Repack an archived file

Repack directly a file stored in `.archive/last`.

This command doesn't invoke `arki-check -f` on the dataset.

**Note**: this command is tested with `ondisk2` datasets only.

    arkitools-cli repack-archived-file /path/to/dataset/.archive/last/2015/01-01.grib1

## List datasets that would acquire a file

    arkitools-cli which-datasets conf myfile.grib1

## Merge new data with archived datasets

`report-merge-data` save the merge in `merged.grib1` and the list of the old
data to delete in `todelete.list`.

    $ arkitools-cli report-merge-data --outfile=merged.grib1 --to-delete-file=todelete.list conf input1.grib1 input2.grib1
    $ xargs -a todelete.list -n 10 -d '\n' rm -v   # remove the files from .archive
    $ arki-scan --dispatch=conf merged.grib1

You can choose the merge type with `-m TYPE`:

- `simple`: the old data are ovewritten by the new ones.
- `vm2flags` (**VM2 only**): only flags are updated.
- `vm2flags-B33196` (**VM2 only**): only the `B33196` flag is updated.

## Delete data from archived datasets

`report-deleted-data` creates a file with the cleared data and print a list of
files to delete.

    $ arkitools-cli report-deleted-data --outfile=cleared.grib1 --to-delete-file=todelete.list conf "product: tp"
    $ xargs -a todelete.list -n 10 -d '\n' rm -v   # remove the files from .archive
    $ arki-scan --dispatch=conf merged.grib1

**Note**: this command delete archived data only. For online data:

    $ arki-query "product: tp" -C conf > todelete.md
    $ arki-check --remove=todelete.md -C conf
