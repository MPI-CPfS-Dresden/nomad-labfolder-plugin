from nomad.config.models.plugins import (
    ParserEntryPoint,
    SchemaPackageEntryPoint,
)
from pydantic import Field


class NewSchemaPackageEntryPoint(SchemaPackageEntryPoint):
    parameter: int = Field(0, description='Custom configuration parameter')

    def load(self):
        from labfolder_plugin.schema_package import m_package

        return m_package


schema_package_entry_point = NewSchemaPackageEntryPoint(
    name='NewSchemaPackage',
    description='New schema package entry point configuration.',
)


class JsonMapperEntryPoint(SchemaPackageEntryPoint):
    parameter: int = Field(0, description='Custom configuration parameter')

    def load(self):
        from labfolder_plugin.jsonimport import m_package

        return m_package


jsonmapper_entry = JsonMapperEntryPoint(
    name='JSON Mapper Importer',
    description='New schema package entry point configuration.',
)


class MappedJsonEntryPoint(SchemaPackageEntryPoint):
    parameter: int = Field(0, description='Custom configuration parameter')

    def load(self):
        from labfolder_plugin.jsonimport import m_package

        return m_package


mappedjson_entry = MappedJsonEntryPoint(
    name='JSON Mapped Importer',
    description='New schema package entry point configuration.',
)


class JsonMapperParserEntryPoint(ParserEntryPoint):
    def load(self):
        from labfolder_plugin.parser import JsonMapperParser

        return JsonMapperParser(**self.dict())


jsonmapper_parser = JsonMapperParserEntryPoint(
    name='MapperParser for Json Mapper files',
    description="""Parser for Json Mapping files.""",
    mainfile_name_re=r'.+\.json',
    mainfile_mime_re='application/json',
    mainfile_contents_re=r'.+json_mapper_class_key',
)


class MappedJsonParserEntryPoint(ParserEntryPoint):
    def load(self):
        from labfolder_plugin.parser import MappedJsonParser

        return MappedJsonParser(**self.dict())


mappedjson_parser = MappedJsonParserEntryPoint(
    name='JsonParser for Json Mapped files',
    description="""Parser for Json Mapped files.""",
    mainfile_name_re=r'.+\.json',
    mainfile_mime_re='application/json',
    mainfile_contents_re=r'.+mapped_json_class_key',
)
