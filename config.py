from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    report_name: str = 'GSE68849'
    base_path: str = 'data'
    output_dir: str = 'data/output'
    temp_dir: str = 'data/temp'
    special_key: str = 'Probes'

    special_cols: list[str] = [
        "Definition",
        "Ontology_Component",
        "Ontology_Process",
        "Obsolete_Probe_Id",
        "Probe_Sequence",
        "Synonyms",
        "Ontology_Function",
    ]

    def archive_name(self, report_name: str):
        return f'{report_name}_RAW.tar'

    def data_url(self, report_name):
        return f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={report_name}&format=file'
