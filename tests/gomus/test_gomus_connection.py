import datetime as dt
import os
import re

from packaging import version
import regex
import requests
from varname import nameof

from db_test import DatabaseTestCase, logger
from _utils import utils

BASE_URL = 'https://barberini.gomus.de/'
EXPECTED_VERSION_LINE_NUMBER = 387
EXPECTED_VERSION_TAG = '4.2.0.15'
GOMUS_SESS_ID = os.environ['GOMUS_SESS_ID']


class TestGomusConnection(DatabaseTestCase):
    """Tests whether the current gomus connection is valid."""

    def test_valid_session_id(self):
        """Test if GOMUS_SESS_ID env variable contains a valid session ID."""
        response = requests.get(
            BASE_URL,
            cookies={'_session_id': os.environ['GOMUS_SESS_ID']},
            allow_redirects=False)
        self.assertEqual(200, response.status_code)

    def test_version(self):
        ln, version_tag = self.search_version()

        self.assertEqual(
            EXPECTED_VERSION_TAG,
            version_tag,
            msg="Gomus version number in HTML string has changed! Please "
                "make sure that all scrapers still work as expected before "
                "updating this test."
        )
        if EXPECTED_VERSION_LINE_NUMBER != ln:
            logger.error(
                f"Gomus HTML format has changed (line number of version tag "
                f"has changed from {EXPECTED_VERSION_LINE_NUMBER} to {ln}), "
                f"but version number is still the same. Did they push some "
                f"undocumented changes?"
            )

    def patch_version(self):
        ln, version_tag = self.search_version()

        with open(__file__, 'r') as file:
            source = file.read()

        source = regex.sub(
            rf"(?<=^{nameof(EXPECTED_VERSION_LINE_NUMBER)} = )\d+$",
            str(ln),
            source,
            flags=regex.MULTILINE)
        source = regex.sub(
            rf"(?<=^{nameof(EXPECTED_VERSION_TAG)} = ').+(?='$)",
            version_tag,
            source,
            flags=regex.MULTILINE)

        with open(__file__, 'w') as file:
            file.write(source)

        return version_tag

    def mr_patch_version(self):
        """Create a GitLab merge request to update the version number."""
        version_tag = self.patch_version()

        import gitlab
        gl = gitlab.Gitlab(url=os.environ['CI_SERVER_URL'],
                           private_token=os.environ['GITLAB_OAUTH_TOKEN'])
        project = gl.projects.get(os.environ['CI_PROJECT_ID'])

        # find any existing MRs for a source branch like update-gomus-version-*
        mrs = project.mergerequests.list(state='opened')
        mrs = [
            {
                "mr": mr,
                "version": re.search(r'v(\d+\.)+\d+$', mr.title).group(0)
            }
            for mr in mrs
            if mr.source_branch.startswith('update-gomus-version-')
        ]
        for mr in mrs:
            # close all MRs for older versions
            if version.parse(mr['version']) < version.parse(version_tag):
                print(f"Closing merge request for version {mr['version']}: "
                      f"{mr['mr'].web_url}")
                mr['mr'].state_event = 'close'
                mr['mr'].save()
            else:
                print(f"Merge request for version {mr['version']} already "
                      f"exists: {mr['mr'].web_url}")
                print("Aborting.")
                return False

        print(f"Creating new patch for version {version_tag}...")
        date = dt.datetime.now().strftime('%Y%m%d')
        branch_name = f'update-gomus-version-{date}'
        changelog_url = ('https://barberini.gomus.de/wiki/spaces/REL/pages/'
                         '243073130')

        import subprocess as sp  # nosec B404
        sp.run(['git', 'checkout', '-b', branch_name], check=True)
        sp.run(['git', 'add', __file__], check=True)
        sp.run(['git',
                '-c', 'user.name=\'GitLab CI\'',
                '-c', 'user.email=\'GitLab CI <noreply@gitlab.com>\'',
                'commit', '-m',
                f"Update gomus version number to v{version_tag}"],
               check=True)
        sp.run(['git', 'remote', 'add', 'origin-token',
                f'https://token:{os.environ["GITLAB_OAUTH_TOKEN"]}@'
                f'{os.environ["CI_SERVER_HOST"]}/'
                f'{os.environ["CI_PROJECT_PATH"]}.git'],
               check=True)
        sp.run(['git', '-c', 'http.sslVerify=false', 'push', 'origin-token',
                branch_name], check=True)

        mr = project.mergerequests.create({
            'source_branch': branch_name,
            'target_branch': 'master',
            'title': f"Update gomus version number to v{version_tag}",
            'description': "Gomus version number has changed to "
                           f"{version_tag}.\n\nGomus changelog: "
                           f"{changelog_url}\n\n"
                           "(This MR was created automatically by a CI job.)",
            'labels': ['autogenerated', 'maintenance'],
            'remove_source_branch': True
        })
        print(f"Created merge request: {mr.web_url}")

        import luigi.notifications
        from _utils.utils import enforce_luigi_notifications
        with enforce_luigi_notifications(format='html'):
            luigi.notifications.send_error_email(
                subject=("Approve update of gomus version"),
                message=("The gomus version number has changed to "
                         f"{version_tag}. The following MR has been created "
                         f"automatically to update the version number in the "
                         f"scrapers: {mr.web_url}")
            )

        sp.run(['git', 'checkout', '-'], check=True)
        return True

    def search_version(self):
        if GOMUS_SESS_ID == '':
            raise ValueError(
                "Please make sure a valid Gomus session ID is provided")

        response = requests.get(
            BASE_URL,
            cookies={'_session_id': GOMUS_SESS_ID})
        response.raise_for_status()

        version_hits = {
            utils.strcoord(response.text, match.start(0))[0]: match
            for match in re.finditer(
                r'''<small class='text-muted' title='go~mus Version'>
(\d+\.\d+\.\d+(?:\.\d+)*(\w+)?)\s+- .+ Edition''',
                response.text
            )
        }
        self.assertTrue(
            version_hits,
            msg="Gomus version number could not be found in HTML string"
        )
        self.assertEqual(
            1,
            len(version_hits),
            msg="Ambiguous version info in gomus HTML"
        )

        ln = list(version_hits.keys())[0]
        version_tag = list(version_hits.values())[0][1]
        return ln, version_tag
