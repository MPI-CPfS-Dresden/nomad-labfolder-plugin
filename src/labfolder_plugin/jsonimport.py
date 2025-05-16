from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    pass


import importlib
import json

from nomad.datamodel import ClientContext
from nomad.datamodel.data import (
    ArchiveSection,
    EntryData,
)
from nomad.datamodel.metainfo.annotations import (
    Rules,
)
from nomad.datamodel.results import ELN, Results
from nomad.metainfo import (
    Quantity,
    SchemaPackage,
    SubSection,
)
from nomad.search import search
from nomad.utils.json_transformer import Transformer
from nomad_material_processing.utils import create_archive
from structlog.stdlib import (
    BoundLogger,
)

m_package = SchemaPackage()


def get_class(class_string, logger):
    try:
        class_obj = getattr(
            importlib.import_module('.'.join(class_string.split('.')[:-1])),
            class_string.split('.')[-1],
        )
        return class_obj
    except AttributeError:
        logger.warning(
            'The module '
            + '.'.join(class_string.split('.')[:-1])
            + ' has no class '
            + class_string.split('.')[-1]
            + '.'
        )
    except ModuleNotFoundError:
        logger.warning(
            'The module ' + '.'.join(class_string.split('.')[:-1]) + ' was not found.'
        )
    return


def createrulesjson(rulesclasses):
    rulesdict = dict()
    for rule in rulesclasses:
        thisrule = dict()
        thisrule.update({'source': rule['source'], 'target': rule['target']})
        if 'default_value' in rule.keys():
            thisrule.update({'default_value': rule['default_value']})
        if 'use_rule' in rule.keys():
            thisrule.update({'use_rule': rule['use_rule']})
        if 'conditions' in rule.keys():
            condlist = []
            for cond in rule['conditions']:
                condlist.append(
                    {
                        cond['name']: {
                            'regex_path': cond['regex_path'],
                            'regex_pattern': cond['regex_pattern'],
                        }
                    }
                )
            thisrule.update({'conditions': condlist})
        rulesdict.update(dict({rule['name']: thisrule}))
    return json.dumps({'rules': rulesdict})


class RuleCondition(ArchiveSection):
    name = Quantity(type=str)
    regex_path = Quantity(type=str, description='Path to data field')
    regex_pattern = Quantity(type=str, description='Regex condition for data field')


class MapperRule(ArchiveSection):
    name = Quantity(type=str)
    source = Quantity(type=str, description='Source of the rule')
    target = Quantity(type=str, description='Target of the rule')
    default_value = Quantity(type=str, description='Default value of the rule')
    use_rule = Quantity(type=str, description='use rule field of the rule')
    conditions = SubSection(section_def=RuleCondition, repeats=True)


class MainMapper(ArchiveSection):
    name = Quantity(type=str)
    path_to_schema = Quantity(
        type=str, description='Path to the schema for the section'
    )
    rules = SubSection(section_def=MapperRule, repeats=True)

    def normalize(self, archive, logger: BoundLogger) -> None:
        super().normalize(archive, logger)


class SubSectionMapper(MainMapper):
    main_key = Quantity(
        type=str, description='Key of the main class, where the SubSectin is linked.'
    )
    is_archive = Quantity(
        type=bool,
        description='Archives will be created separately and linked only as reference',
    )
    repeats = Quantity(
        type=bool,
        description='Marks a repeatable Subsection, attaches to existing list.',
    )

    def normalize(self, archive, logger: BoundLogger) -> None:
        super().normalize(archive, logger)


class JsonMapper(EntryData, ArchiveSection):
    mapper_key = Quantity(type=str, description='Key to match with the imported JSON')
    main_mapping = SubSection(section_def=MainMapper)
    subsection_mappings = SubSection(section_def=SubSectionMapper, repeats=True)
    mapper_file = Quantity(
        type=str,
        a_eln=dict(component='FileEditQuantity'),
        a_browser=dict(adaptor='RawFileAdaptor'),
    )

    def normalize(self, archive, logger: BoundLogger) -> None:
        super().normalize(archive, logger)

        if not archive.results:
            archive.results = Results(eln=ELN())
        if not archive.results.eln:
            archive.results.eln = ELN()
        archive.results.eln.sections = ['JsonMapper']

        if self.mapper_file:
            with archive.m_context.raw_file(self.mapper_file, 'r') as file:
                jsonfile = json.load(file)

            try:
                self.mapper_key = jsonfile['json_mapper_class_key']
                archive.results.eln.names = [self.mapper_key]
            except KeyError:
                logger.error(
                    'Missing keys for jsonmapper file (json_mapper_class_key).'
                )
            logger.info('Starting search for already existing mappers with same key.')
            if not isinstance(archive.m_context, ClientContext):
                search_result = search(
                    owner='all',
                    query={
                        'results.eln.sections:any': ['JsonMapper'],
                        'results.eln.names:any': [self.mapper_key],
                    },
                    user_id=archive.metadata.main_author.user_id,
                )
                if len(search_result.data) > 0:
                    logger.error(
                        'At least one mapper with the same key has been found.'
                    )

            subsections = []
            for key in jsonfile.keys():
                if key == 'json_mapper_class_key':
                    continue
                subsection = jsonfile[key]
                if 'is_main' in subsection and subsection['is_main'] == 'True':
                    sectionclass = MainMapper()
                    if (
                        'main_key' in subsection
                        or 'is_archive' in subsection
                        or 'repeats' in subsection
                    ):
                        logger.error(
                            'Main section of json mapper should not contain main_key or is_archive or repeats.'
                        )
                else:
                    sectionclass = SubSectionMapper()
                    try:
                        sectionclass.main_key = subsection['main_key']
                    except KeyError:
                        logger.error(f'main_key is missing from Subsection {key}.')
                    if 'is_archive' in subsection:
                        sectionclass.is_archive = subsection['is_archive']
                    if 'repeats' in subsection:
                        sectionclass.repeats = subsection['repeats']
                sectionclass.name = key
                try:
                    sectionclass.path_to_schema = subsection['schema']
                except KeyError:
                    logger.error(f'schema is missing from Subsection {key}.')
                if 'rules' in subsection:
                    rules = []
                    for rulekey in subsection['rules'].keys():
                        rule = subsection['rules'][rulekey]
                        rulesection = MapperRule()
                        try:
                            if not (
                                'source' in rule.keys() and 'target' in rule.keys()
                            ):
                                logger.error(
                                    f'Rule {rulekey} in SubSection {key} is missing source or target key.'
                                )
                            rulesection.name = rulekey
                            rulesection.source = rule['source']
                            rulesection.target = rule['target']
                            if 'default_value' in rule.keys():
                                rulesection.default_value = rule['default_value']
                            if 'use_rule' in rule.keys():
                                rulesection.use_rule = rule['use_rule']
                            if 'conditions' in rule.keys():
                                condlist = []
                                for condition in rule['conditions']:
                                    conditionssection = RuleCondition()
                                    condname = next(iter(condition))
                                    conditionssection.name = condname
                                    conditionssection.regex_path = condition[condname][
                                        'regex_path'
                                    ]
                                    conditionssection.regex_pattern = condition[
                                        condname
                                    ]['regex_pattern']
                                    condlist.append(conditionssection)
                                rulesection.conditions = condlist
                        except AttributeError:
                            rulesection.name = f'{rulekey}_to_{rule}'
                            rulesection.source = rulekey
                            rulesection.target = rule
                        rules.append(rulesection)
                    sectionclass.rules = rules
                else:
                    logger.warning(
                        f'Rules section is missing from Subsection {key}. No mapping will be done.'
                    )
                sectionclass.normalize(archive, logger)
                if 'is_main' in subsection:
                    if self.main_mapping == None:
                        self.main_mapping = sectionclass
                    else:
                        logger.error('is_main can only be in one Subsection.')
                else:
                    subsections.append(sectionclass)
                logger.info(sectionclass.m_to_dict())
            self.subsection_mappings = subsections
            if self.main_mapping == None:
                logger.error('No main mapping found.')


class MappedJson(EntryData, ArchiveSection):
    json_file = Quantity(
        type=str,
        a_eln=dict(component='FileEditQuantity'),
        a_browser=dict(adaptor='RawFileAdaptor'),
    )
    mapper_key = Quantity(type=str, description='Key to map with the mapper schema')

    def normalize(self, archive, logger: BoundLogger) -> None:
        super().normalize(archive, logger)
        if self.json_file:
            with archive.m_context.raw_file(self.json_file, 'r') as file:
                jsonfile = json.load(file)

            try:
                self.mapper_key = jsonfile['mapped_json_class_key']
            except KeyError:
                logger.error(
                    'Missing keys for mappedjson file (mapped_json_class_key).'
                )

        logger.info('Starting search for mapper with same key.')
        if not isinstance(archive.m_context, ClientContext):
            search_result = search(
                owner='all',
                query={
                    'results.eln.sections:any': ['JsonMapper'],
                    'results.eln.names:any': [self.mapper_key],
                },
                user_id=archive.metadata.main_author.user_id,
            )
            if len(search_result.data) > 1:
                logger.error('Two or more mappers were found.')
            elif len(search_result.data) == 1:
                logger.info(search_result.data[0]['data'])
                mapper = search_result.data[0]['data']
            else:
                logger.error('No mapper was found.')

        mainrules = {
            'main_transformation': Rules(
                **json.loads(createrulesjson(mapper['main_mapping']['rules']))
            )
        }
        maintransformer = Transformer(mainrules)
        transformed_main = maintransformer.transform(jsonfile, 'main_transformation')

        mainclass = get_class(mapper['main_mapping']['path_to_schema'], logger)()
        mainclass.m_update_from_dict(transformed_main)

        for i in range(len(mapper['subsection_mappings'])):
            submapping = mapper['subsection_mappings'][i]
            subclass = get_class(submapping['path_to_schema'], logger)()
            subrules = {
                'sub_transformation': Rules(
                    **json.loads(createrulesjson(submapping['rules']))
                )
            }
            subtransformer = Transformer(subrules)
            transformed_sub = subtransformer.transform(jsonfile, 'sub_transformation')
            tempunits = transformed_sub.pop('tempunits', None)
            subclass.m_update_from_dict(transformed_sub)
            if tempunits:
                for unitkey in tempunits.keys():
                    from pint import UnitRegistry

                    ureg = UnitRegistry(autoconvert_offset_to_baseunit=True)
                    setattr(
                        subclass,
                        unitkey,
                        subclass[unitkey].magnitude * ureg(tempunits[unitkey]),
                    )
            if 'is_archive' in submapping.keys() and submapping['is_archive']:
                sub_ref = create_archive(
                    subclass,
                    archive,
                    subclass.name + '.archive.json',
                )
                setattr(mainclass, submapping['main_key'], sub_ref)
            elif 'repeats' in submapping.keys() and submapping['repeats']:
                mainclass[submapping['main_key']].append(subclass)
            else:
                setattr(mainclass, submapping['main_key'], subclass)

        create_archive(
            mainclass,
            archive,
            mainclass.name + '.archive.json',
        )


m_package.__init_metainfo__()
