#
# Copyright The NOMAD Authors.
#
# This file is part of NOMAD. See https://nomad-lab.eu for further info.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import (
    TYPE_CHECKING,
)

from nomad.datamodel import EntryArchive
from nomad.datamodel.data import (
    EntryData,
)
from nomad.datamodel.metainfo.annotations import (
    ELNAnnotation,
)
from nomad.metainfo import Quantity
from nomad.parsing import MatchingParser

if TYPE_CHECKING:
    from nomad.datamodel.datamodel import (
        EntryArchive,
    )

from nomad.datamodel import EntryArchive
from nomad_measurements.utils import create_archive

from labfolder_plugin.jsonimport import (
    JsonMapper,
    MappedJson,
)


class JsonMapperFile(EntryData):
    mapper = Quantity(
        type=JsonMapper,
        a_eln=ELNAnnotation(
            component='ReferenceEditQuantity',
        ),
    )


class MappedJsonFile(EntryData):
    mapper = Quantity(
        type=MappedJson,
        a_eln=ELNAnnotation(
            component='ReferenceEditQuantity',
        ),
    )


class JsonMapperParser(MatchingParser):
    def set_entrydata_definition(self):
        self.entrydata_definition = JsonMapper

    def parse(self, mainfile: str, archive: EntryArchive, logger) -> None:
        self.set_entrydata_definition()
        data_file = mainfile.split('/')[-1]
        data_file_with_path = mainfile.split('raw/')[-1]
        entry = self.entrydata_definition()
        entry.mapper_file = data_file_with_path
        file_name = f'{data_file[:-4]}.archive.json'
        archive.data = JsonMapperFile(mapper=create_archive(entry, archive, file_name))
        archive.metadata.entry_name = data_file + ' mapper file'


class MappedJsonParser(MatchingParser):
    def set_entrydata_definition(self):
        self.entrydata_definition = MappedJson

    def parse(self, mainfile: str, archive: EntryArchive, logger) -> None:
        self.set_entrydata_definition()
        data_file = mainfile.split('/')[-1]
        data_file_with_path = mainfile.split('raw/')[-1]
        entry = self.entrydata_definition()
        entry.json_file = data_file_with_path
        file_name = f'{data_file[:-4]}.archive.json'
        archive.data = MappedJsonFile(mapper=create_archive(entry, archive, file_name))
        archive.metadata.entry_name = data_file + ' json file'
