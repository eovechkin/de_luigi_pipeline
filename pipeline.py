import luigi
import requests
import pandas as pd
import tarfile
import gzip
import io
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
from luigi import Task, LocalTarget
from config import Settings

cfg = Settings()


class ArchiveDownloadTask(Task):
    '''
    Download the report archive from given URL.    
    '''

    report_name = cfg.report_name
    temp_dir = cfg.temp_dir
    data_source = cfg.data_url(report_name)
    arch_name = cfg.archive_name(report_name)
    arch_fullpath = f"{temp_dir}/{arch_name}"

    def run(self):
        # getting file from site
        response = requests.get(self.data_source, allow_redirects=True)
        response.raise_for_status()

        with open(self.arch_fullpath, "wb") as file:
            file.write(response.content)

    def output(self):
        return LocalTarget(self.arch_fullpath)


class ArchiveExtractTask(Task):
    '''
    Exctact downloaded archive with data.
    '''

    report_name = cfg.report_name
    temp_dir = cfg.temp_dir
    extracted_files_list = f"{temp_dir}/extracted_files.txt"

    def requires(self):
        return ArchiveDownloadTask()

    def run(self):
        members_to_extract = []
        extracted_files = []

        # extracting main archive
        with tarfile.open(self.input().path, "r") as tar:
            for member in tar.getmembers():
                member_path = f'{self.temp_dir}/{member.name.strip(".txt.gz")}'

                members_to_extract.append({
                    'path': member_path,
                    'name': member.name
                })

                tar.extract(member=member, path=member_path, filter='data')

        # extracting each member archive
        for m in members_to_extract:
            src_file = f"{m['path']}/{m['name']}"
            dst_file = f"{m['path']}/{m['name'].strip('.txt.gz')}.txt"

            with gzip.open(src_file) as gz:
                with open(dst_file, 'wb') as f:
                    f.write(gz.read())
                    extracted_files.append(dst_file)

        # writing target file to the output file
        with open(self.extracted_files_list, "w") as f:
            f.write("\n".join(extracted_files))

    def output(self):
        return LocalTarget(self.extracted_files_list)


class SplittingTablesTask(Task):
    '''
    Splitting and trucating tsv file to target state.
    '''

    report_name = cfg.report_name
    temp_dir = cfg.temp_dir
    target_dir = cfg.output_dir
    clean_mark_filename = f"{temp_dir}/can_be_cleaned.txt"

    # special keys for cutting some info
    special_key = cfg.special_key
    special_cols = cfg.special_cols

    def requires(self):
        return ArchiveExtractTask()

    def run(self):
        datasets = defaultdict(list)

        with open(str(self.input())) as f:
            exctracted_files = map(lambda f: f.strip(), f.readlines())

        # extracting separate datasets
        for filename in exctracted_files:
            dfs = self._process_file(filename)

            for key, df in dfs.items():
                datasets[key].append(df)

        # joinind dataset using pandas
        dataframes = {}
        for key, dfs in datasets.items():
            df = pd.concat(dfs)
            dataframes[key] = df

        self._save_dataframes(pd=dataframes)
        self._ext_processing(pd=dataframes)

        # setting done flag
        Path(self.clean_mark_filename).touch()

    def output(self):
        return LocalTarget(self.clean_mark_filename)

    def _process_file(self, filename: str):
        '''
        Splitting given file into separate part of datasets.       
        '''

        dfs = {}

        with open(filename) as f:
            write_key = None
            fio = io.StringIO()

            for l in f.readlines():
                if l.startswith('['):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                    fio = io.StringIO()
                    write_key = l.strip('[]\n')
                    continue

                if write_key:
                    fio.write(l)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep='\t')

        return dfs

    def _save_dataframes(self, pd, ext='full'):
        '''
        Save pandas dataframes into output tsv files.
        '''

        for key, df in pd.items():
            filename = f"{key}_{datetime.now(tz=timezone.utc)}_{ext}.tsv"
            filename = filename.replace(" ", "_")
            filepath = f"{self.target_dir}/{filename}"
            df.to_csv(filepath, sep="\t", index=False)

    def _ext_processing(self, pd):
        '''
        Processing special key by dropping special features.
        '''

        df = pd[self.special_key]
        df.drop(columns=self.special_cols, inplace=True)
        self._save_dataframes(pd={self.special_key: df}, ext="short")


class CleanupTask(Task):
    '''
    Cleanup temporary working directory. Checking clean up mark.
    '''

    temp_dir = cfg.temp_dir

    def requires(self):
        return SplittingTablesTask()

    def run(self):
        for name in os.listdir(self.temp_dir):
            path = f"{self.temp_dir}/{name}"

            if os.path.isfile(path):
                os.remove(f"{self.temp_dir}/{name}")
            else:
                for file in os.listdir(path):
                    os.remove(f"{path}/{file}")

                os.rmdir(f"{self.temp_dir}/{name}")


def main():

    # check folders exist
    if not os.path.exists(cfg.base_path):
        os.mkdir(cfg.base_path)

    if not os.path.exists(cfg.output_dir):
        os.mkdir(cfg.output_dir)

    if not os.path.exists(cfg.temp_dir):
        os.mkdir(cfg.temp_dir)

    # starting luigi pipeline
    luigi.build([
        ArchiveDownloadTask(),
        ArchiveExtractTask(),
        SplittingTablesTask(),
        CleanupTask(),
    ],
        workers=1,
        local_scheduler=True,
        no_lock=False)

    luigi.run()


if __name__ == '__main__':
    main()
