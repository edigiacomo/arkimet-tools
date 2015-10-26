# arkitools/dataset - dataset utilities
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
import os.path
import shutil
import subprocess
import configparser
import re
from datetime import datetime, timedelta


def validate_dataset(ds):
    """Raise exception if the given path is not a valid dataset."""
    if not os.path.isdir(ds):
        raise Exception("Invalid dataset: {} is not a directory".format(ds))
    if not os.path.exists(os.path.join(ds, "config")):
        raise Exception("Invalid dataset: {}/config not found".format(ds))


def clone_dataset(src_ds, dst_ds):
    """Clone a dataset, without the data."""
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


def which_datasets(infiles, dsconf):
    """Given a mergeconf, return the datasets that would acquire the
    files as a dict."""
    cfg = configparser.ConfigParser()
    cfg.read([dsconf])
    for s in cfg.keys():
        section = dict(cfg.items(s))
        if "filter" not in section or "path" not in section:
            continue
        f = section.get("filter")
        r = subprocess.check_output(["arki-query", "--summary", "--dump",
                                     f] + infiles)
        if r and not r.isspace():
            yield section


def guess_step_from_path(path):
    """Guess datasets step from path of one of its files. Return None if cannot
    guess."""
    if re.match('^.*/(\d{4})/(\d{2})-(\d{2}).*$', path):
        return "daily"
    else:
        return None


def is_archived_file_within_timeinterval(path, begin, end, step=None):
    """Check if the archived file is within the given timeinterval using, if
    possible, the file path as reftime metadata. If step is None, try to guess
    its step."""
    # yearly: YY/YYYY
    # monthly: YYYY/mm
    # biweekly: YYYY/mm-{1,2}
    # weekly: YYYY/mm-{1,2,3,4,5}
    # daily: YYYY/mm-dd
    # singlefile: YYYY/mm/dd/HH
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


def is_generic_file_within_timeinterval(path, begin, end):
    """Check if file is within timeinterval, using arki-query."""
    from subprocess import check_output, DEVNULL
    q = "reftime:>={},<={}".format(begin.isoformat(), end.isoformat())
    r = check_output(["arki-query", "--summary", "--dump", q, path])
    return r and not r.isspace()


def is_file_within_timeinterval(path, begin, end, archived=False, step=None):
    """Check if file is within timeinterval."""
    if archived:
        try:
            return archived_file_within_timeinterval(path, begin, end, step)
        except:
            return generic_file_within_timeinterval(path, begin, end)
    else:
        return generic_file_within_timeinterval(path, begin, end)


def repack_archived_file(infile, backup_file=None, dry_run=False, tmpbasedir=None):
    """Repack an archived file."""
    from subprocess import check_call, DEVNULL
    from glob import glob
    from tempfile import TemporaryDirectory

    with TemporaryDirectory(dir=tmpbasedir) as tmpdir:
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


def do_clone_dataset(args):
    from arkitools.dataset import clone_dataset

    return clone_dataset(src_ds=args.srcds, dst_ds=args.dstds)
