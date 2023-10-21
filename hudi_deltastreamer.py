# from pyhocon import ConfigFactory
from hudi.deltastreamer import DeltaStreamer
config = ConfigFactory.parse_file("delta_streamer_config.conf")
streamer = DeltaStreamer(config)
streamer.start()
streamer.await_termination()