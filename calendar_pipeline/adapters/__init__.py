from .baker_hughes import BakerHughesRigCountAdapter
from .bls import BlsScheduleAdapter
from .cftc_cot import CftcCotScheduleAdapter
from .ecb import EcbMeetingCalendarAdapter
from .eia import EiaScheduleAdapter
from .eurostat import EurostatReleaseCalendarAdapter
from .fed_fomc import FedFomcCalendarAdapter
from .ons_rss import OnsReleaseCalendarAdapter
from .usda_nass import UsdaNassCalendarAdapter


def default_adapters():
    return [
        EiaScheduleAdapter(),
        UsdaNassCalendarAdapter(),
        FedFomcCalendarAdapter(),
        EcbMeetingCalendarAdapter(),
        EurostatReleaseCalendarAdapter(),
        OnsReleaseCalendarAdapter(),
        BlsScheduleAdapter(),
        BakerHughesRigCountAdapter(),
        CftcCotScheduleAdapter(),
    ]
