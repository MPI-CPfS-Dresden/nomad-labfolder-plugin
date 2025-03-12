from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from nomad.datamodel.datamodel import (
        EntryArchive,
    )
    from structlog.stdlib import (
        BoundLogger,
    )

import importlib

import pandas as pd
from nomad.config import config
from nomad.datamodel.data import (
    ElnIntegrationCategory,
)
from nomad.datamodel.metainfo.annotations import (
    BrowserAnnotation,
    ELNAnnotation,
    ELNComponentEnum,
    SectionProperties,
)
from nomad.datamodel.metainfo.eln.labfolder import LabfolderProject
from nomad.metainfo import (
    Quantity,
    SchemaPackage,
    Section,
)
from nomad.units import ureg
from nomad_material_processing.utils import (
    create_archive,
)

configuration = config.get_plugin_entry_point(
    'labfolder_plugin.schema_packages:schema_package_entry_point'
)

m_package = SchemaPackage()


class LabFolderImport(LabfolderProject):
    m_def = Section(
        label='General Labfolder Project Import',
        categories=[ElnIntegrationCategory],
        a_eln=ELNAnnotation(
            properties=SectionProperties(
                order=[
                    'project_url',
                    'import_entry_id',
                    'labfolder_email',
                    'password',
                    'mapping_file',
                ],
            ),
            lane_width='800px',
        ),
    )

    name = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
    )

    import_entry_id = Quantity(
        type=str,
        a_eln=ELNAnnotation(
            component='StringEditQuantity',
        ),
    )

    mapping_file = Quantity(
        type=str,
        description="""
        The file with the schema mapping (optional). (.dat file).
        """,
        a_browser=BrowserAnnotation(adaptor='RawFileAdaptor'),
        a_eln=ELNAnnotation(component='FileEditQuantity'),
    )

    def normalize(self, archive: 'EntryArchive', logger: 'BoundLogger') -> None:  # noqa: PLR0912, PLR0915
        super().normalize(archive, logger)

        import re

        TAG_RE = re.compile(r'<[^>]+>')

        _selection_mapping = dict()

        if self.mapping_file:
            if self.mapping_file.endswith('.json'):
                import json

                with archive.m_context.raw_file(self.mapping_file, 'r') as mapping:
                    inp = json.load(mapping)
            elif self.mapping_file.endswith('.yaml'):
                import yaml

                with archive.m_context.raw_file(self.mapping_file, 'r') as mapping:
                    inp = yaml.safe_load(mapping)
            else:
                logger.error(
                    'The mapping file has an unsuitable format. Please use a \
                             json or yaml file.'
                )
            for cl in inp['Classes'].keys():
                try:
                    _selection_mapping[cl] = getattr(
                        importlib.import_module(
                            '.'.join(inp['Classes'][cl]['class'].split('.')[:-1])
                        ),
                        inp['Classes'][cl]['class'].split('.')[-1],
                    )
                except AttributeError:
                    logger.warning(
                        'The module '
                        + '.'.join(inp['Classes'][cl]['class'].split('.')[:-1])
                        + ' has no class '
                        + inp['Classes'][cl]['class'].split('.')[-1]
                        + '.'
                    )
                    continue
                except ModuleNotFoundError:
                    logger.warning(
                        'The module '
                        + '.'.join(inp['Classes'][cl]['class'].split('.')[:-1])
                        + ' was not found.'
                    )
                    continue

        if self.import_entry_id:
            for entry in self.entries:
                if str(entry.id) == str(self.import_entry_id):
                    # More general way possible?
                    possible_classes = list(set(_selection_mapping) & set(entry.tags))
                    if len(possible_classes) == 0:
                        logger.warning(
                            'No suitable class found in the LabFolderEntry tags. \
                            Use one of the following: '
                            + str(list(_selection_mapping))
                        )
                        break
                    if len(possible_classes) > 1:
                        logger.warning(
                            'Too many suitable class found in the LabFolderEntry tags. Use only one of the following: '  # noqa: E501
                            + str(list(_selection_mapping))  # noqa: E501
                        )
                        break
                    labfolder_section = _selection_mapping[possible_classes[0]]()
                    logger.info(possible_classes[0] + ' template found.')
                    data_content = dict()
                    table_content = []
                    text_content = dict()
                    for element in entry.elements:
                        if element.element_type == 'DATA':
                            data_content = data_content | element.labfolder_data
                        if element.element_type == 'TEXT':
                            text_content = text_content | dict(
                                {
                                    TAG_RE.sub(
                                        '', element.content.split('</p>')[0]
                                    ): TAG_RE.sub(
                                        '', ';'.join(element.content.split('</p>')[1:])
                                    )
                                }
                            )
                        if element.element_type == 'TABLE':
                            for key in element.content['sheets']:
                                table = element.content['sheets'][key]['data'][
                                    'dataTable'
                                ]
                                df = pd.DataFrame.from_dict(
                                    {
                                        (i): {
                                            (j): table[i][j]['value']
                                            for j in table[i].keys()
                                        }
                                        for i in table.keys()
                                    },
                                    orient='index',
                                )
                                df.columns = df.iloc[0]
                                df = df.iloc[1:].reset_index(drop=True)
                                df = df.to_dict(orient='index')
                                df['name'] = element.title
                                table_content.append(df)

                    for section in inp['Classes'].keys():
                        logger.info(section)
                        replist = []
                        for repcount in range(10):
                            found = True
                            if (
                                inp['Classes'][section]['repeats'] == 'false'
                                or inp['Classes'][section]['repeats'] == 'true'
                            ) and repcount > 0:
                                continue
                            if (
                                inp['Classes'][section]['type'] == 'SubSection'
                                or inp['Classes'][section]['type'] == 'Archive'
                                or inp['Classes'][section]['type'] == 'main'
                            ):
                                if (
                                    not hasattr(
                                        labfolder_section,
                                        inp['Classes'][section]['attribute'],
                                    )
                                    and not inp['Classes'][section]['type'] == 'main'
                                ):  # noqa: E501
                                    logger.warning(
                                        'The schema does not have an attribute '
                                        + inp['Classes'][section]['attribute']
                                    )  # noqa: E501
                                    break
                                if inp['Classes'][section]['type'] == 'SubSection':
                                    logger.info('Creating Subsection ' + section)
                                if inp['Classes'][section]['type'] == 'Archive':
                                    logger.info('Creating Archive ' + section)
                                if inp['Classes'][section]['type'] == 'main':
                                    section_object = labfolder_section
                                else:
                                    section_object = getattr(
                                        importlib.import_module(
                                            '.'.join(
                                                inp['Classes'][section]['class'].split(
                                                    '.'
                                                )[:-1]
                                            )
                                        ),
                                        inp['Classes'][section]['class'].split('.')[-1],
                                    )()  # noqa: E501

                                # TODO: find better way of interating through a json
                                maps = inp['Mapping']['Data elements']
                                for key1 in maps:
                                    if 'object' in maps[key1]:
                                        if maps[key1]['object'] == section:
                                            try:
                                                setattr(
                                                    section_object,
                                                    maps[key1]['key'],
                                                    ureg.Quantity(
                                                        float(
                                                            data_content[key1]['value']
                                                        ),
                                                        data_content[key1]['unit'],
                                                    ),
                                                )  # noqa: E501
                                            except Exception:
                                                try:
                                                    setattr(
                                                        section_object,
                                                        maps[key1]['key'],
                                                        data_content[key1][
                                                            'description'
                                                        ],
                                                    )
                                                except Exception as error:
                                                    logger.warning(
                                                        'JSON entry with key '
                                                        + key1
                                                        + ' could not be parsed with error: '  # noqa: E501
                                                        + str(error)
                                                    )  # noqa: E501
                                                    continue
                                        continue
                                    maps1 = maps[key1]
                                    for key2 in maps1:
                                        if 'object' in maps1[key2]:
                                            if maps1[key2]['object'] == section:
                                                try:
                                                    setattr(
                                                        section_object,
                                                        maps1[key2]['key'],
                                                        ureg.Quantity(
                                                            float(
                                                                data_content[key1][
                                                                    key2
                                                                ]['value']
                                                            ),
                                                            data_content[key1][key2][
                                                                'unit'
                                                            ],
                                                        ),
                                                    )  # noqa: E501
                                                except Exception:
                                                    try:
                                                        setattr(
                                                            section_object,
                                                            maps1[key2]['key'],
                                                            data_content[key1][key2][
                                                                'description'
                                                            ],
                                                        )  # noqa: E501
                                                    except Exception as error:
                                                        logger.warning(
                                                            'JSON entry with key '
                                                            + key1
                                                            + key2
                                                            + ' could not be parsed with error: '  # noqa: E501
                                                            + str(error)
                                                        )  # noqa: E501
                                                        continue
                                            continue
                                        maps2 = maps1[key2]
                                        for key3 in maps2:
                                            if 'object' in maps2[key3]:
                                                if maps2[key3]['object'] == section:
                                                    try:
                                                        setattr(
                                                            section_object,
                                                            maps2[key3]['key'],
                                                            ureg.Quantity(
                                                                float(
                                                                    data_content[key1][
                                                                        key2
                                                                    ][key3]['value']
                                                                ),
                                                                data_content[key1][
                                                                    key2
                                                                ][key3]['unit'],
                                                            ),
                                                        )  # noqa: E501
                                                    except Exception:
                                                        try:
                                                            setattr(
                                                                section_object,
                                                                maps2[key3]['key'],
                                                                data_content[key1][
                                                                    key2
                                                                ][key3]['description'],
                                                            )  # noqa: E501
                                                        except Exception as error:
                                                            logger.warning(
                                                                'JSON entry with key '
                                                                + key1
                                                                + key2
                                                                + key3
                                                                + ' could not be parsed with error: '  # noqa: E501
                                                                + str(error)
                                                            )  # noqa: E501
                                                            continue
                                                    continue

                                maps = inp['Mapping']['Text elements']
                                for key1 in maps:
                                    if maps[key1]['object'] == section:
                                        try:
                                            setattr(
                                                section_object,
                                                maps[key1]['key'],
                                                text_content[key1],
                                            )  # noqa: E501
                                        except Exception as error:
                                            logger.warning(
                                                'Text entry with key '
                                                + key1
                                                + ' could not be parsed with error: '
                                                + str(error)
                                            )  # noqa: E501

                                maps = inp['Mapping']['Table elements']
                                for key1 in maps:
                                    for table in table_content:
                                        if table['name'] == key1:
                                            try:
                                                line = table[repcount]
                                                for key2 in maps[key1]:
                                                    if (
                                                        maps[key1][key2]['object']
                                                        == section
                                                    ):  # noqa: E501
                                                        try:
                                                            setattr(
                                                                section_object,
                                                                maps[key1][key2]['key'],
                                                                line[key2],
                                                            )
                                                        except Exception as error:
                                                            logger.warning(
                                                                'Text entry with key '
                                                                + key1
                                                                + key2
                                                                + ' could not be parsed with error: '  # noqa: E501
                                                                + str(error)
                                                            )  # noqa: E501

                                            except KeyError:
                                                found = False

                                if inp['Classes'][section]['name'] != '':
                                    temp = inp['Classes'][section]['name']
                                    name = ''
                                    for line in temp.split('+'):
                                        if line.startswith('LF'):
                                            if line.startswith('LF.data'):
                                                add = data_content
                                                for i in range(
                                                    len(line.split('.')) - 2
                                                ):
                                                    add = add[line.split('.')[i + 2]]
                                                name = name + add['description']
                                        else:
                                            name = name + line

                                    section_object.name = name

                                if inp['Classes'][section]['type'] == 'Archive':
                                    section_object = create_archive(
                                        section_object, archive, name
                                    )

                                if found or repcount == 0:
                                    replist.append(section_object)
                                logger.info(section, repcount, replist)

                        if not inp['Classes'][section]['type'] == 'main':
                            if inp['Classes'][section]['repeats'] == 'false':
                                setattr(
                                    labfolder_section,
                                    inp['Classes'][section]['attribute'],
                                    replist[0],
                                )
                            else:
                                setattr(
                                    labfolder_section,
                                    inp['Classes'][section]['attribute'],
                                    replist,
                                )

                    temp = inp['Classes'][possible_classes[0]]['name']
                    name = ''
                    for line in temp.split('+'):
                        if line.startswith('LF'):
                            if line.startswith('LF.data'):
                                add = data_content
                                for i in range(len(line.split('.')) - 2):
                                    add = add[line.split('.')[i + 2]]
                                name = name + add['description']
                        else:
                            name = name + line

                    labfolder_section.name = name

                    create_archive(labfolder_section, archive, name)


m_package.__init_metainfo__()
