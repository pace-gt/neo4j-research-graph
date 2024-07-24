"""
Download.py: Holds the Download class, which contains method necessary
to download json files from OpenAlex
"""
__author__ = "Keller Smith"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"
import json 
import requests
import os
import sys
class Download:
    """
    A class for an openalex download

    Imports
    -------
    json
    requests
    os
    sys

    Methods
    -------
    download_based_on_las_known():
        Download all authors into json files based on last known institution as Georgia Tech
    download_based_on_works():
        Filters openalex works based on GT and collects necessary information
    add_author(id):
        Adds a single author and their relevant information
    add_work(id):
        Adds a single work and relevant information
    create_files():
        Creates 4 json files to collect information
    close_files():
        Closes the files so that they're in proper json format
    """
    def __init__(self):
        """
        Constructs attributes for a SlurmJob object
        
        Parameters
        ----------
        None
        """
        self.added_institutes = []
        self.added_works = []
        self.missed_authors = []
        self.missed_works = []    
    def download_based_on_last_known(self):
        """
            Download all authors into json files based on last known institution as Georgia Tech
            Puts authors in openalex_author_dump.json
            Puts their collaborators in openalex_collab_dump.json
            Puts their institutions in openalex_institution_dump.json
            Puts their works in openalex_work_dump.json

            Requirements
            ------------
            None

            Parameters
            ----------
            None

            Returns
            -------
            None
        """
        headers = {'Accept': 'application/json'}
        with open('openalex_author_dump.json', 'a') as a, open('openalex_institution_dump.json', 'a') as i, open('openalex_work_dump.json', 'a') as w, open('openalex_collab_dump.json', 'a') as c:
            retry = True
            count = 0
            while retry == True:
                try:
                    response = requests.get('https://api.openalex.org/authors?per-page=200&filter=last_known_institution.ror:https://ror.org/01zkghx44&cursor=*', headers=headers)
                    data = response.json()
                    retry = False
                except requests.exceptions.Timeout as e:
                    if count > 2:
                        print("Timeout Error")
                        raise SystemExit(e)
                    else:
                        count += 1
                except requests.exceptions.ConnectionError as e:
                    print("There was a connection error.")
                    raise SystemExit(e)
                except requests.exceptions.JSONDecodeError as e:
                    if count > 2:
                        print("JSON error")
                        raise SystemExit(e)
                    else:
                        count += 1
                except requests.exceptions.RequestException as e:
                    raise SystemExit(e)
            authors = data['results']
            if data['meta']['count'] == 0:
                print("There are no authors in openalex. The database may be updating.")
                raise(requests.exceptions.ConnectionError())
            next_page = data['meta']['next_cursor']
            while next_page is not None:
                retry = True
                count = 0
                try:
                    for to_add in authors:
                        a.write(json.dumps(to_add) + ",")
                        works_url = to_add['works_api_url']
                        work_response = requests.get(works_url, headers=headers).json()
                        if not (work_response['meta']['count'] == 0):
                            works = work_response['results']
                            for add_work in works:
                                if not (add_work == {}):
                                    work_id = add_work['id']
                                    if not (work_id in self.added_works):
                                        w.write(json.dumps(add_work) + ",")
                                        for authorship in add_work['authorships']:
                                            author = authorship['author']
                                            author['past_institutions'] = []
                                            for p_i in authorship['institutions']:
                                                if not(p_i == {}):
                                                    author['past_institutions'].append(p_i)
                                                    id = p_i['id']
                                                    if not (id in self.added_institutes):
                                                        i.write(json.dumps(p_i) + ",")
                                                        self.added_institutes.append(id)
                                                c.write(json.dumps(author) + ",")
                except:
                    print("Author missed")
                    if 'id' in to_add:
                        self.missed_authors.append(to_add['id'])
                while retry == True:
                    try:
                        response = requests.get('https://api.openalex.org/authors?per-page=200&filter=last_known_institution.ror:https://ror.org/01zkghx44&cursor=' + next_page, headers=headers)
                        data = response.json()
                        retry = False
                    except requests.exceptions.Timeout as e:
                        if count > 2:
                            print("Timeout Error")
                            raise SystemExit(e)
                        else:
                            count += 1
                    except requests.exceptions.ConnectionError as e:
                        print("There was a connection error.")
                        raise SystemExit(e)
                    except requests.exceptions.JSONDecodeError as e:
                        if count > 2:
                            print("JSON error")
                            raise SystemExit(e)
                        else:
                            count += 1
                    except requests.exceptions.RequestException as e:
                        raise SystemExit(e)
                authors = data['results']
                next_page = data['meta']['next_cursor']
            if not (authors == []):
                try:
                    for to_add in authors:
                        a.write(json.dumps(to_add) + ",")
                        works_url = to_add['works_api_url']
                        work_response = requests.get(works_url, headers=headers).json()
                        if not (work_response['meta']['count'] == 0):
                            works = work_response['results']
                            for add_work in works:
                                if not (add_work == {}):
                                    work_id = add_work['id']
                                    if not (work_id in self.added_works):
                                        w.write(json.dumps(add_work) + ",")
                                        for authorship in add_work['authorships']:
                                            author = authorship['author']
                                            author['past_institutions'] = []
                                            for p_i in authorship['institutions']:
                                                if not(p_i == {}):
                                                    author['past_institutions'].append(p_i)
                                                    id = p_i['id']
                                                    if not (id in self.added_institutes):
                                                        i.write(json.dumps(p_i) + ",")
                                                        self.added_institutes.append(id)
                                                c.write(json.dumps(author) + ",")
                except:
                    if 'id' in to_add:
                        self.missed_authors.append(to_add['id'])
    def download_based_on_works(self):
        """
            Filters openalex works based on GT and collects necessary information
            Puts all collaborators in openalex_collab_dump.json
            Puts their institutions in openalex_institution_dump.json
            Puts their works in openalex_work_dump.json

            Requirements
            ------------
            None

            Parameters
            ----------
            None

            Returns
            -------
            None
        """
        headers = {'Accept': 'application/json'}
        with open('openalex_author_dump.json', 'a') as a, open('openalex_institution_dump.json', 'a') as i, open('openalex_work_dump.json', 'a') as w, open('openalex_collab_dump.json', 'a') as c:
            retry = True
            count = 0
            while retry == True:
                try:
                    response = requests.get('https://api.openalex.org/works?filter=institutions.ror:https://ror.org/01zkghx44&per-page=200&cursor=*', headers=headers)
                    data = response.json()
                    retry = False
                except requests.exceptions.Timeout as e:
                    if count > 2:
                        print("Timeout Error")
                        raise SystemExit(e)
                    else:
                        count += 1
                except requests.exceptions.ConnectionError as e:
                    print("There was a connection error.")
                    raise SystemExit(e)
                except requests.exceptions.JSONDecodeError as e:
                    if count > 2:
                        print("JSON error")
                        raise SystemExit(e)
                    else:
                        count += 1
                except requests.exceptions.RequestException as e:
                    raise SystemExit(e)
            
            works = data['results']
            next_page = data['meta']['next_cursor']

            while next_page is not None:
                retry = True
                count = 0
                for add_work in works:
                    try:
                        if not (add_work == {}):
                            work_id = add_work['id']
                            if not (work_id in self.added_works):
                                w.write(json.dumps(add_work) + ",")
                                for authorship in add_work['authorships']:
                                    author = authorship['author']
                                    author['past_institutions'] = []
                                    for p_i in authorship['institutions']:
                                        if not(p_i == {}):
                                            author['past_institutions'].append(p_i)
                                            id = p_i['id']
                                            if not (id in self.added_institutes):
                                                i.write(json.dumps(p_i) + ",")
                                                self.added_institutes.append(id)
                                        c.write(json.dumps(author) + ",")
                    except:
                        if 'id' in add_work:
                            self.missed_works.append(add_work['id'])
                while retry == True:
                    try:
                        response = requests.get('https://api.openalex.org/works?filter=institutions.ror:https://ror.org/01zkghx44&per-page=200&cursor=' + next_page, headers=headers)
                        data = response.json()
                        retry = False
                    except requests.exceptions.Timeout as e:
                        if count > 2:
                            print("Timeout Error")
                            raise SystemExit(e)
                        else:
                            count += 1
                    except requests.exceptions.ConnectionError as e:
                        print("There was a connection error.")
                        raise SystemExit(e)
                    except requests.exceptions.JSONDecodeError as e:
                        if count > 2:
                            print("JSON error")
                            raise SystemExit(e)
                        else:
                            count += 1
                    except requests.exceptions.RequestException as e:
                        raise SystemExit(e)
                    works = data['results']
                    next_page = data['meta']['next_cursor']
            if not (works == []):
                for add_work in works:
                    if not (add_work == {}):
                        work_id = add_work['id']
                        if not (work_id in self.added_works):
                            w.write(json.dumps(add_work) + ",")
                            for authorship in add_work['authorships']:
                                author = authorship['author']
                                author['past_institutions'] = []
                                for p_i in authorship['institutions']:
                                    if not(p_i == {}):
                                        author['past_institutions'].append(p_i)
                                        id = p_i['id']
                                        if not (id in self.added_institutes):
                                            i.write(json.dumps(p_i) + ",")
                                            self.added_institutes.append(id)
                                    c.write(json.dumps(author) + ",")
    def add_author(self, id):
        """
            Adds a single author and their relevant information
            Puts authors in openalex_author_dump.json
            Puts their collaborators in openalex_collab_dump.json
            Puts their institutions in openalex_institution_dump.json
            Puts their works in openalex_work_dump.json

            Requirements
            ------------
            None

            Parameters
            ----------
            id: str
                Openalex id of the author to be added

            Returns
            -------
            None
        """
        headers = {'Accept': 'application/json'}
        with open('openalex_author_dump.json', 'a') as a, open('openalex_institution_dump.json', 'a') as i, open('openalex_work_dump.json', 'a') as w, open('openalex_collab_dump.json', 'a') as c:
            response = requests.get('https://api.openalex.org/authors?filter=openalex:' + str(id), headers=headers)
            to_add = response.json()
            to_add = to_add['results'][0]
            works_url = to_add['works_api_url']
            work_response = requests.get(works_url).json()
            if not (work_response['meta']['count'] == 0):
                works = work_response['results']
                for add_work in works:
                    if not (add_work == {}):
                        work_id = add_work['id']
                        if not (work_id in self.added_works):
                            w.write(json.dumps(add_work) + ",")
                            for authorship in add_work['authorships']:
                                author = authorship['author']
                                author['past_institutions'] = []
                                for p_i in authorship['institutions']:
                                    if not(p_i == {}):
                                        author['past_institutions'].append(p_i)
                                        id = p_i['id']
                                        if not (id in self.added_institutes):
                                            i.write(json.dumps(p_i) + ",")
                                            self.added_institutes.append(id)
                                    c.write(json.dumps(author) + ",")
    def add_work(self, id):
        """
            Adds a single work and relevant information
            Puts all collaborators in openalex_collab_dump.json
            Puts their institutions in openalex_institution_dump.json
            Puts their works in openalex_work_dump.json

            Requirements
            ------------
            None

            Parameters
            ----------
            id: str
                Openalex id of the work to be added

            Returns
            -------
            None
        """
        headers = {'Accept': 'application/json'}
        with open('openalex_author_dump.json', 'a') as a, open('openalex_institution_dump.json', 'a') as i, open('openalex_work_dump.json', 'a') as w, open('openalex_collab_dump.json', 'a') as c:
            response = requests.get('https://api.openalex.org/works?filter=openalex:' + str(id), headers=headers)
            add_work = response.json()
            add_work = add_work['results'][0]
            if not (add_work == {}):
                w.write(json.dumps(add_work) + ",")
                for authorship in add_work['authorships']:
                    author = authorship['author']
                    author['past_institutions'] = []
                    for p_i in authorship['institutions']:
                        if not(p_i == {}):
                            author['past_institutions'].append(p_i)
                            i.write(json.dumps(p_i) + ",")
                        c.write(json.dumps(author) + ",")
    def create_files(self):
        """
            Creates 4 json files to collect information
            openalex_author_dump.json
            openalex_collab_dump.json
            openalex_institution_dump.json
            openalex_work_dump.json

            Requirements
            ------------
            None

            Parameters
            ----------
            None

            Returns
            -------
            None
        """
        with open('openalex_author_dump.json', 'a') as f:
            f.write('{"results": [')
        with open('openalex_work_dump.json', 'a') as f:
            f.write('{"results": [')
        with open('openalex_institution_dump.json', 'a') as f:
            f.write('{"results": [')
        with open("openalex_collab_dump.json", 'a') as f:
            f.write('{"results": [')
    def close_files(self):
        """
        Closes the files so that they're in proper json format

        Requirements
        ------------
        openalex_author_dump.json
        openalex_collab_dump.json
        openalex_institution_dump.json
        openalex_work_dump.json

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        with open('openalex_author_dump.json', 'rb+') as filehandle:
            filehandle.seek(-1, os.SEEK_END)
            filehandle.truncate()
        with open('openalex_author_dump.json', 'a') as f:
            f.write(']}')
        with open('openalex_work_dump.json', 'rb+') as filehandle:
            filehandle.seek(-1, os.SEEK_END)
            filehandle.truncate()
        with open('openalex_work_dump.json', 'a') as f:
            f.write(']}')
        with open('openalex_institution_dump.json', 'rb+') as filehandle:
            filehandle.seek(-1, os.SEEK_END)
            filehandle.truncate()
        with open('openalex_institution_dump.json', 'a') as f:
            f.write(']}')
        with open('openalex_collab_dump.json', 'rb+') as filehandle:
            filehandle.seek(-1, os.SEEK_END)
            filehandle.truncate()
        with open('openalex_collab_dump.json', 'a') as f:
            f.write(']}')
def main():
    """
        Combines the above methods to create files, download everything, and close the files
        If any authors or works were unable to be added initially, attempts to add them
        again at the end
        Prints relevant information about successes and failures to the output file.

        Requirements
        ------------
        None

        Parameters
        ----------
        None

        Returns
        -------
        None
    """
    download = Download()
    download.create_files()
    print("Files Created")
    download.download_based_on_last_known()
    print("Download 1 Complete")
    download.download_based_on_works()
    print("Download 2 Complete")
    if not (download.missed_authors == []):
        print("The following authors were unable to be added: " + str(download.missed_authors))
        print("Attempting to add now.")
    for item in download.missed_authors:
        download.add_author(item)
    print("All authors successfully added!")
    if not (download.missed_works == []):
        print("The following works were unable to be added: " + str(download.missed_works))
        print("Attempting to add now.")
    for item in download.missed_works:
        download.add_work(item)
    print("All works successfully added!")
    print("Full download complete.")
    download.close_files()

if __name__ == "__main__":
    main()