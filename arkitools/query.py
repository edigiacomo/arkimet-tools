class TrangeGrib1Expander(object):
    """Expand a group of GRIB1 timeranges in an Arkimet query."""
    def __init__(self):
        self.items = []

    def query(self):
        """Return the arkimet query."""
        return "timerange: " + " or ".join(self.items)

    def __str__(self):
        return self.query()

    def add(self, tty, start_step, end_step, step, from_0=True):
        """Add a new group of timerange
        :param int tty: time range type
        :param int start_step: first forecast step
        :param int end_step: last forecast step
        :param int step: value of the forecast step
        :param bool from_0: time intervals start from 0

        :return: the object itself (to chain multiple add)
        """
        steps = range(start_step, end_step + step, step) if all([
            start_step is not None,
            end_step is not None,
            step is not None,
        ]) else []

        if tty == 1:
            # Only analysis is possible
            self.items.append("GRIB1,1,0h")
        elif tty == 0:
            # Forecast at a specified reference time
            steps = range(start_step, end_step + step, step)
            self.items += list("GRIB1,0,{}h".format(v) for v in steps)
        elif 1 < tty < 6:
            steps = range(start_step, end_step + step, step)
            # forecast valid over a time interval
            if from_0:
                # time interval start from 0
                self.items += list("GRIB1,{},0h,{}h".format(tty, v)
                                   for v in steps)
            else:
                # use successive, non-overlapping intervals
                self.items += list("GRIB1,{},{}h,{}h".format(tty, v[0], v[1])
                                   for v in ((s, s+step) for s in steps))

        return self


class LevelGrib1Expander(object):
    def __init__(self):
        self.items = []

    def query(self):
        return "level: " + " or ".join(self.items)

    def __str__(self):
        return self.query()

    def add(self, lty, start_step=None, end_step=None, step=1):
        if end_step is None:
            end_step = start_step
        
        if start_step is not None:
            steps = range(start_step, end_step + step, step)

        if any([start_step is None, lty > 0 and lty < 10,
                lty in [102, 200, 201]]):
            # absolute levels or no value specified
            self.items.append("GRIB1,{}".format(lty))
        elif lty in [20, 100, 103, 105, 107, 109, 111, 113, 115, 117, 119, 125, 160]:
            # specified levels
            self.items += list("GRIB1,{},{}".format(lty, v) for v in steps)
        elif lty in [101, 104, 106, 108, 112, 114, 116, 120, 121, 128, 141]:
            # layer between two specified levels
            self.items += list("GRIB1,{},{},{}".format(lty, v[0], v[1])
                               for v in ((s, s+step) for s in steps))

        return self
