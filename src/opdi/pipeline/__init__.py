"""The pipeline steps that turn position reports into flights and events.

* step 02, :class:`TrackProcessor` (:mod:`opdi.pipeline.tracks`): tracks;
* step 03, :class:`FlightListProcessor` (:mod:`opdi.pipeline.flights`): the
  flight list;
* step 04, :class:`FlightEventProcessor` (:mod:`opdi.pipeline.events`): the
  events and measurements, with one module per event family;
* step 04b (:mod:`opdi.pipeline.flight_list_step`): the main milestones on
  the flight list.
"""

from opdi.pipeline.events import FlightEventProcessor
from opdi.pipeline.flights import FlightListProcessor
from opdi.pipeline.tracks import TrackProcessor

__all__ = [
    "TrackProcessor",
    "FlightListProcessor",
    "FlightEventProcessor",
]
