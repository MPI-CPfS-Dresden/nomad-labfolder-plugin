from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    pass


import json
import re
from urllib.parse import parse_qs, urlparse

import requests
import yaml
from nomad.config import config
from nomad.datamodel.data import (
    ElnIntegrationCategory,
    EntryData,
)
from nomad.datamodel.metainfo.annotations import (
    ELNAnnotation,
)
from nomad.metainfo import (
    Quantity,
    SchemaPackage,
    Section,
)
from nomad_material_processing.utils import create_archive

from labfolder_plugin.jsonimport import MappedJson

configuration = config.get_plugin_entry_point(
    'labfolder_plugin:schema_package_entry_point'
)

m_package = SchemaPackage()

_element_type_path_mapping = {
    'TEXT': 'text',
    'FILE': 'file',
    'IMAGE': 'image',
    'DATA': 'data',
    'TABLE': 'table',
    'WELL_PLATE': 'well-plate',
}


def restructure_data(labfolder_structure):
    simpler_structure = dict()
    for line in labfolder_structure:
        if line['type'] == 'DATA_ELEMENT_GROUP':
            b = dict()
            for line2 in line['children']:
                if line2['type'] == 'DESCRIPTIVE_DATA_ELEMENT':
                    if line2['description']:
                        b.update(
                            {line2['title'].replace(' ', '_'): line2['description']}
                        )
                if line2['type'] == 'SINGLE_DATA_ELEMENT':
                    if line2['value']:
                        b.update(
                            {
                                line2['title'].replace(' ', '_'): {
                                    'value': line2['value'],
                                    'unit': line2['unit'],
                                }
                            }
                        )
                if line2['type'] == 'DATA_ELEMENT_GROUP':
                    c = dict()
                    for line3 in line['children']:
                        if line3['type'] == 'DESCRIPTIVE_DATA_ELEMENT':
                            if line3['description']:
                                c.update(
                                    {
                                        line3['title'].replace(' ', '_'): line3[
                                            'description'
                                        ]
                                    }
                                )
                        if line3['type'] == 'SINGLE_DATA_ELEMENT':
                            if line3['value']:
                                c.update(
                                    {
                                        line3['title'].replace(' ', '_'): {
                                            'value': line3['value'],
                                            'unit': line3['unit'],
                                        }
                                    }
                                )
                    b.update({line2['title']: c})
            simpler_structure.update({line['title'].replace(' ', '_'): b})
        if line['type'] == 'DESCRIPTIVE_DATA_ELEMENT':
            if line['description']:
                simpler_structure.update(
                    {line['title'].replace(' ', '_'): line['description']}
                )
        if line['type'] == 'SINGLE_DATA_ELEMENT':
            if line['value']:
                simpler_structure.update(
                    {
                        line['title'].replace(' ', '_'): {
                            'value': line['value'],
                            'unit': line['unit'],
                        }
                    }
                )
    return simpler_structure


def restructure_table(labfolder_structure):
    simpler_structure = dict()
    allsheets = dict()
    for sheet in labfolder_structure['content']['sheets'].keys():
        tmpsheet = labfolder_structure['content']['sheets'][sheet]
        tmp = dict()
        # Assume Table structure with header in first line
        for row in tmpsheet['data']['dataTable']['0'].keys():
            tmprow = []
            for col in tmpsheet['data']['dataTable'].keys():
                if col == '0':
                    continue
                try:
                    tmprow.append(tmpsheet['data']['dataTable'][col][row]['value'])
                except KeyError:
                    tmprow.append('')
            tmp.update(
                {
                    tmpsheet['data']['dataTable']['0'][row]['value'].replace(
                        ' ', '_'
                    ): tmprow
                }
            )
        allsheets.update({tmpsheet['name'].replace(' ', '_'): tmp})
    simpler_structure.update(
        {labfolder_structure['title'].replace(' ', '_'): allsheets}
    )
    return simpler_structure


def expandrules(json):
    rules = json['transformer']['rules']
    for key in rules.keys():
        try:
            rules[key].keys()
        except AttributeError:
            json['transformer']['rules'][key] = {'source': key, 'target': rules[key]}
    return json


class LabfolderImportError(Exception):
    pass


class LabFolderImport(EntryData):
    m_def = Section(
        label='Labfolder Project Import', categories=[ElnIntegrationCategory]
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__headers = None
        self.logger = None

    project_url = Quantity(type=str, a_eln=dict(component='StringEditQuantity'))
    import_entry_id = Quantity(
        type=str,
        a_eln=ELNAnnotation(
            component='StringEditQuantity',
        ),
    )
    labfolder_email = Quantity(type=str, a_eln=dict(component='StringEditQuantity'))
    password = Quantity(
        type=str,
        a_eln=dict(component='StringEditQuantity', props=dict(type='password')),
    )

    mapper_key = Quantity(type=str, a_eln=dict(component='StringEditQuantity'))

    def _labfolder_api_method(
        self, method, url, msg='cannot do labfolder api request', **kwargs
    ):
        response = method(
            f'{self._api_base_url}{url}', headers=self._headers, timeout=10, **kwargs
        )

        if response.status_code >= 400:  # noqa: PLR2004
            self.logger.error(
                msg, data=dict(status_code=response.status_code, text=response.text)
            )
            raise LabfolderImportError()

        return {} if url.endswith('/logout') else response

    @property
    def _api_base_url(self):
        match = re.match(r'^(.+)/eln/notebook.*$', self.project_url)
        if not match:
            self.logger.error(
                'unexpected labfolder url format',
                data=dict(project_url=self.project_url),
            )
            raise LabfolderImportError()

        return f'{match.group(1)}/api/v2'

    @property
    def _headers(self):
        if not self.__headers:
            response = requests.post(
                f'{self._api_base_url}/auth/login',
                json=dict(user=self.labfolder_email, password=self.password),
            )

            if response.status_code != 200:  # noqa: PLR2004
                self._clear_user_data()
                self.logger.error(
                    'cannot login',
                    data=dict(status_code=response.status_code, text=response.text),
                )
                raise LabfolderImportError()

            self.__headers = dict(Authorization=f'Bearer {response.json()["token"]}')

        return self.__headers

    def _clear_user_data(self):
        self.labfolder_email = None
        self.password = None

        archive = self.m_root()
        with archive.m_context.raw_file(archive.metadata.mainfile, 'wt') as f:
            if archive.metadata.mainfile.endswith('json'):
                json.dump(dict(data=archive.data.m_to_dict()), f)
            else:
                yaml.dump(dict(data=archive.data.m_to_dict()), f)

    def normalize(self, archive, logger):  # noqa: PLR0912, PLR0915
        super().normalize(archive, logger)
        self.logger = logger

        if (
            not self.project_url
            or not self.import_entry_id
            or not self.labfolder_email
            or not self.password
        ):
            logger.error('missing information, cannot import project')
            raise LabfolderImportError()

        try:
            project_ids = parse_qs(urlparse(self.project_url).fragment[1:])[
                'projectIds'
            ]
        except KeyError as e:
            logger.error('cannot parse project ids from url', exc_info=e)
            raise LabfolderImportError()

        entries = self._labfolder_api_method(
            requests.get, f'/entries?project_ids={",".join(project_ids)}'
        ).json()

        content = dict()

        for entry in entries:
            if entry['id'] == self.import_entry_id:
                for element in entry['elements']:
                    element_type = element['type']

                    if element_type not in _element_type_path_mapping:
                        logger.warn(
                            'unknown element type', data=dict(element_type=element_type)
                        )
                        continue

                    data = self._labfolder_api_method(
                        requests.get,
                        f'/elements/{_element_type_path_mapping[element_type]}/{element["id"]}/version/{element["version_id"]}',
                    ).json()

                    if element_type == 'TEXT':
                        TAG_RE = re.compile(r'<[^>]+>')
                        content_data = dict(
                            {
                                TAG_RE.sub(
                                    '',
                                    data['content'].split('</p>')[0].replace(' ', '_'),
                                ): TAG_RE.sub(
                                    '', ';'.join(data['content'].split('</p>')[1:])
                                )
                            }
                        )
                    elif element_type == 'DATA':
                        content_data = restructure_data(data['data_elements'])
                    elif element_type == 'TABLE':
                        content_data = restructure_table(data)
                    else:
                        logger.warning(
                            f'LabFolder element {element_type} is not yet supported.'
                        )

                    content.update(content_data)

        content.update({'nomadclass': 'mappedjson', 'mapper_key': self.mapper_key})

        logger.info(content)

        filename = (
            f'labfolder_project_{project_ids[0]}_entry_{self.import_entry_id}.json'
        )
        with archive.m_context.raw_file(filename, 'w') as outfile:
            outfile.write(json.dumps(content))

        toparse = MappedJson(json_file=filename)

        create_archive(toparse, archive, filename.replace('.json', '.archive.json'))

        # Resetting Token and Logging out: Invalidating all access tokens
        self._clear_user_data()
        self._labfolder_api_method(requests.post, '/auth/logout')


m_package.__init_metainfo__()
