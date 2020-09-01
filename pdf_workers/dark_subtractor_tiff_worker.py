import argparse
from functools import partial
import pprint

import msgpack
import msgpack_numpy as mpn

import bluesky_kafka
import databroker
from event_model import RunRouter, unpack_event_page
import ophyd.sim


# from bluesky_darkframes import DarkSubtraction
import event_model
import copy
import time
import numpy

# mpn.patch() is recommended by msgpack-numpy
# as the way to patch msgpack for numpy
mpn.patch()


class DarkSubtraction(event_model.DocumentRouter):
    """Document router to do in-place background subtraction.

    Expects that the events are filled.

    The values in `(light_stream_name, field)` are replaced with ::

        np.clip(light - np.clip(dark - pedestal, 0), 0)


    Adds the key f'{self.field}_is_background_subtracted' to the
    'light_stream_name' stream and a configuration key for the
    pedestal value.


    .. warning

       This mutates the document stream in-place!

    Parameters
    ----------
    field : str
        The name of the field to do the background subtraction on.

        This field must contain the light-field values in the
        'light-stream' and the background images in the 'dark-stream'

    light_stream_name : str, optional
         The stream that contains the exposed images that need to be
         background subtracted.

         defaults to 'primary'

    dark_stream_name : str, optional
         The stream that contains the background dark images.

         defaults to 'dark'

    pedestal : int, optional
         Pedestal to add to the data to make sure subtracted result does not
         fall below 0.

         This is actually pre subtracted from the dark frame for efficiency.

         Defaults to 100.
    """

    def __init__(
        self, field, light_stream_name="primary", dark_stream_name="dark", pedestal=0
    ):
        self.field = field
        self.light_stream_name = light_stream_name
        self.dark_stream_name = dark_stream_name
        self.light_descriptor = None
        self.dark_descriptor = None
        self.dark_frame = None
        self.pedestal = pedestal

    def descriptor(self, doc):
        if doc["name"] == self.light_stream_name:
            self.light_descriptor = doc["uid"]
            # add flag that we did the background subtraction
            doc = copy.deepcopy(dict(doc))
            doc["data_keys"][f"{self.field}_is_background_subtracted"] = {
                "source": "DarkSubtraction",
                "dtype": "number",
                "shape": [],
                "precsion": 0,
                "object_name": f"{self.field}_DarkSubtraction",
            }
            doc["configuration"][f"{self.field}_DarkSubtraction"] = {
                "data": {"pedestal": self.pedestal},
                "timestamps": {"pedestal": time.time()},
                "data_keys": {
                    "pedestal": {
                        "source": "DarkSubtraction",
                        "dtype": "number",
                        "shape": [],
                        "precsion": 0,
                    }
                },
            }
            doc["object_keys"][f"{self.field}_DarkSubtraction"] = [
                f"{self.field}_is_background_subtracted"
            ]

        elif doc["name"] == self.dark_stream_name:
            self.dark_descriptor = doc["uid"]
        return doc

    def event_page(self, doc):
        if doc["descriptor"] == self.dark_descriptor:
            (self.dark_frame,) = numpy.asarray(doc["data"][self.field], dtype=float)
            self.dark_frame -= self.pedestal
            numpy.clip(self.dark_frame, a_min=0, a_max=None, out=self.dark_frame)
        elif doc["descriptor"] == self.light_descriptor:
            if self.dark_frame is None:
                raise Exception(
                    "DarkSubtraction has not received a 'dark' Event yet, so "
                    "it has nothing to subtract."
                )
            doc = copy.deepcopy(dict(doc))
            light = numpy.asarray(doc["data"][self.field], dtype=float)
            subtracted = self.subtract(light, self.dark_frame)
            doc["data"][self.field] = subtracted
            doc["data"][f"{self.field}_is_background_subtracted"] = [True]
            doc["timestamps"][f"{self.field}_is_background_subtracted"] = [time.time()]
        return doc

    def subtract(self, light, dark):
        return numpy.clip(light - dark, a_min=0, a_max=None).astype(light.dtype)


def my_filename(out, my_samplename, my_iter):
    this_entry = out
    cen_x_val = this_entry["center_Grid_X"]
    cen_y_val = this_entry["Grid_Y"]
    cen_x_str = "%08.4f" % cen_x_val
    cen_y_str = "%08.4f" % cen_y_val
    my_iter_name = "%05i" % my_iter

    file_name = my_samplename
    file_name += "_" + str(my_iter_name)
    file_name += "_X" + str(cen_x_str)
    file_name += "_Y" + str(cen_y_str)
    file_name += ".tiff"

    return file_name


from tifffile import imsave
from pathlib import Path


##def export_subtracted_tiff_series(header, file_path, *args, **kwargs):
def export_subtracted_tiff_series(name, doc, export_dir, my_sample_name, subtractor):
    print(f"export_subtracted_tiff_series name: {name}")
    out = []
    ##subtractor = DarkSubtraction("pe1c_image")
    ##my_samplename = None
    file_written_list = []
    export_dir_path = Path(export_dir) / Path(my_sample_name)
    export_dir_path.mkdir(parents=True, exist_ok=True)
    ##for name, doc in header.documents(fill=True):
    name, doc = subtractor(name, doc)
    ##if name == "start":
    ##    my_samplename = doc["md"]

    if name == "event_page":
        for event_doc in unpack_event_page(doc):
            # if 'pe1c_is_background_subtracted' in doc['data']:
            if "Grid_Y" in event_doc["data"]:
                # print(list(doc['data']))

                # out.append({'image': doc['data']['pe1c_image'],
                #         'center_Grid_X': (doc['data']['start_Grid_X'] + doc['data']['stop_Grid_X']) / 2,
                #         **{k: doc['data'][k] for k in ('start_Grid_X', 'stop_Grid_X', 'Grid_Y','pe1c_stats1_total')}})
                # def my_filename(out, my_samplename, my_iter):

                out = {
                    "image": event_doc["data"]["pe1c_image"],
                    "center_Grid_X": (
                        event_doc["data"]["start_Grid_X"] + event_doc["data"]["stop_Grid_X"]
                    )
                    / 2,
                    **{
                        k: event_doc["data"][k]
                        for k in (
                            "start_Grid_X",
                            "stop_Grid_X",
                            "Grid_Y",
                            "pe1c_stats1_total",
                        )
                    },
                }
                this_filename = my_filename(out, my_sample_name, event_doc["seq_num"])
                file_written_list.append(this_filename)
                print("\nwheee " + str(this_filename))
                imsave(
                    str(export_dir_path / this_filename), data=out["image"].astype("int32")
                )
                # break #remove later
    return file_written_list


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--export-dir", required=True, help="output directory for files"
    )
    # arg_parser.add_argument(
    #     "--mongodb-uri", required=True, help="URI for mongo database"
    # )
    arg_parser.add_argument(
        "--kafka-bootstrap-servers",
        required=False,
        default="cmb01:9092,cmb02:9092,cmb03:9092",
        help="comma-separated list of Kafka broker host:port",
    )
    arg_parser.add_argument(
        "--kafka-topics",
        required=False,
        default="pdf.bluesky.documents",
        type=lambda comma_sep_list: comma_sep_list.split(","),
        help="comma-separated list of Kafka topics from which bluesky documents will be consumed",
    )

    args = arg_parser.parse_args()
    pprint.pprint(args)
    start(**vars(args))


def start(export_dir, kafka_bootstrap_servers, kafka_topics):
    def factory(name, start_doc, export_dir):
        my_sample_name = start_doc["md"]
        subtractor = DarkSubtraction("pe1c_image")
        return [
            partial(
                export_subtracted_tiff_series,
                export_dir=export_dir,
                my_sample_name=my_sample_name,
                subtractor=subtractor
            )
        ], []

    dispatcher = bluesky_kafka.RemoteDispatcher(
        topics=kafka_topics,
        group_id="pdf-dark-subtractor-tiff-worker",
        bootstrap_servers=kafka_bootstrap_servers,
        deserializer=msgpack.loads,
    )

    rr = RunRouter(
        [partial(factory, export_dir=export_dir)],
        handler_registry={
            "AD_TIFF": databroker.assets.handlers.AreaDetectorTiffHandler,
            "NPY_SEQ": ophyd.sim.NumpySeqHandler,
        },
    )
    dispatcher.subscribe(rr)
    dispatcher.start()


if __name__ == "__main__":
    main()
