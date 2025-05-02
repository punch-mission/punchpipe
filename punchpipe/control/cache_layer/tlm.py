# import os
# from collections.abc import Callable
# import io
# from typing import Dict
#
# from ccsdspy.utils import split_by_apid
#
# from punchpipe.control.cache_layer import manager
# from punchpipe.control.cache_layer.loader_base_class import LoaderABC
#
# def open_and_split_packet_file(path: str) -> dict[int, io.BytesIO]:
#     with open(path, "rb") as mixed_file:
#         stream_by_apid = split_by_apid(mixed_file)
#     return stream_by_apid
#
# def parse_telemetry_file(path, defs, apid_name2num):
#     success = True
#     contents = open_and_split_packet_file(path)
#     parsed = {}
#     for packet_name in defs:
#         apid_num = apid_name2num[packet_name]
#         if apid_num in contents:
#             try:
#                 parsed[packet_name] = defs[packet_name].load(contents[apid_num], include_primary_header=True)
#             except (ValueError, RuntimeError):
#                 success = False
#     return parsed, success
#
# class TLMLoader(LoaderABC[Dict]):
#     def __init__(self, path: str, defs, apid_name2num):
#         self.path = path
#         self.defs = defs
#         self.apid_name2num = apid_name2num
#
#     def gen_key(self) -> str:
#         return str(hash(f"tlm-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"))
#
#     def src_repr(self) -> str:
#         return self.path
#
#     def load_from_disk(self):
#         try:
#             parsed, _ = parse_telemetry_file(self.path, self.defs, self.apid_name2num)
#         except Exception as e:
#             parsed = None
#         return parsed
#
#     def __repr__(self):
#         return f"TLM({self.path})"
#
#
# def wrap_if_appropriate(psf_path: str, defs, apid_name2num) -> str | Callable:
#     if manager.caching_is_enabled():
#         return TLMLoader(psf_path, defs, apid_name2num).load
#     return psf_path
