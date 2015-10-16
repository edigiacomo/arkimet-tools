# arkimet tools

Tools for [arkimet](https://github.com/ARPA-SIMC/arkimet).

## Clone an empty dataset

```console
python3 arkitools.py clone-dataset src dst
```

## Repack an archived file

Repack directly a file stored in `.archive/last`.

This command doesn't invoke `arki-check -f` on the dataset.

**Note**: this command is tested with `ondisk2` datasets only.

```console
python3 arkitools.py repack-archived-file file
```
