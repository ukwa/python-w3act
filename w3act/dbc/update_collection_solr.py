# -*- coding: utf-8 -*-
import json
import logging

logger = logging.getLogger(__name__)


class UpdateCollectionsSolr(luigi.Task):
    task_namespace = 'discovery'
    date = luigi.DateMinuteParameter(default=datetime.datetime.now())
    solr_endpoint = luigi.Parameter(default='http://localhost:8983/solr/collections')

    def requires(self):
        return [TargetList(self.date), CollectionList(self.date), SubjectList(self.date)]

    def output(self):
        return state_file(self.date,'access-data', 'updated-collections-solr.json')

    @staticmethod
    def add_collection(s, targets_by_id, col, parent_id):
        if col['publish']:
            print("Publishing...", col['name'])

            # add a document to the Solr index
            s.add([
                {
                    "id": col["id"],
                    "type": "collection",
                    "name": col["name"],
                    "description": col["description"],
                    "parentId": parent_id
                }
            ], commit=False)

            # Look up all Targets within this Collection and add them.
            for tid in col.get('target_ids',[]):
                target = targets_by_id.get(tid, None)
                if not target:
                    logger.error("Warning! Could not find target %i" % tid)
                    continue

                # Skip items with no URLs:
                if len(target.get('urls',[])) == 0:
                    continue

                # Determine license status:
                licenses = []
                if target.get('isOA', False):
                    licenses = target.get("license_ids",[])
                    # Use a special value to indicate an inherited license:
                    if len(licenses) == 0:
                        licenses = ['1000']

                # add a document to the Solr index
                s.add([{
                    "id": "cid:%i-tid:%i" % (col['id'], target['id']),
                    "type": "target",
                    "parentId": col['id'],
                    "title": target["title"],
                    "description": target["description"],
                    "url": target["urls"][0],
                    "additionalUrl": target["urls"][1:],
                    "language": target["language"],
                    "startDate": target["crawl_start_date"],
                    "endDate": target["crawl_end_date"],
                    "licenses": licenses
                }], commit=False)

            # Add child collections
            for cc in col["children"]:
                UpdateCollectionsSolr.add_collection(s, targets_by_id, cc, col['id'])
        else:
            print("Skipping...", col['name'])

        return

    def run(self):
        targets = json.load(self.input()[0].open())
        collections = json.load(self.input()[1].open())
        subjects = json.load(self.input()[2].open())

        # build look-up table for Target IDs
        targets_by_id = {}
        target_count = 0
        for target in targets:
            tid = target['id']
            targets_by_id[tid] = target
            target_count += 1
        logger.info("Found %i targets..." % target_count)

        s = pysolr.Solr(self.solr_endpoint, timeout=30)

        # First, we delete everything (!)
        s.delete(q="*:*", commit=False)

        # Update the collections:
        for col in collections:
            UpdateCollectionsSolr.add_collection(s, targets_by_id, col, None)

        # Now commit all changes:
        s.commit()

        # Record that we have completed this task successfully:
        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(collections, indent=4)))


if __name__ == '__main__':
    #luigi.run(['discovery.UpdateCollectionsSolr',  '--date', '2017-04-28', '--local-scheduler'])
    luigi.run(['discovery.PopulateCollectionsSolr', '--local-scheduler'])
