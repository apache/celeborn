#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utility for creating well-formed pull request merges and pushing them to Apache
# Celeborn.
#   usage: dev/merge_pr.py    (see config env vars below)
#
# This utility assumes you already have a local Celeborn git folder and that you
# have added remotes corresponding to both (i) the github apache Celeborn
# mirror and (ii) the apache git repo.

import json
import os
import re
import subprocess
import sys
import traceback
from urllib.request import urlopen
from urllib.request import Request
from urllib.error import HTTPError


try:
    import jira.client

    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

# Location of your Celeborn git development area
PROJECT_HOME = os.environ.get("PROJECT_HOME", os.getcwd())
# Remote name which points to the GitHub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache")
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")
# ASF JIRA username
ASF_USERNAME = os.environ.get("ASF_USERNAME", "")
# ASF JIRA password
ASF_PASSWORD = os.environ.get("ASF_PASSWORD", "")
# ASF JIRA access token
# If it is configured, username and password are dismissed
# Go to https://issues.apache.org/jira/secure/ViewProfile.jspa -> Personal Access Tokens for
# your own token management.
JIRA_ACCESS_TOKEN = os.environ.get("JIRA_ACCESS_TOKEN")
# OAuth key used for issuing requests against the GitHub API. If this is not defined, then requests
# will be unauthenticated. You should only need to configure this if you find yourself regularly
# exceeding your IP's unauthenticated request rate limit. You can create an OAuth key at
# https://github.com/settings/tokens. This script only requires the "public_repo" scope.
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")


GITHUB_BASE = "https://github.com/apache/celeborn/pull"
GITHUB_API_BASE = "https://api.github.com/repos/apache/celeborn"
JIRA_BASE = "https://issues.apache.org/jira/browse"
JIRA_API_BASE = "https://issues.apache.org/jira"
# Prefix added to temporary branches
BRANCH_PREFIX = "PR_TOOL"


def get_json(url):
    try:
        request = Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
        return json.load(urlopen(request))
    except HTTPError as e:
        if "X-RateLimit-Remaining" in e.headers and e.headers["X-RateLimit-Remaining"] == "0":
            print(
                "Exceeded the GitHub API rate limit; see the instructions in "
                + "dev/merge_pr.py to configure an OAuth token for making authenticated "
                + "GitHub requests."
            )
        else:
            print("Unable to fetch URL, exiting: %s" % url)
        sys.exit(-1)


def fail(msg):
    print(msg)
    clean_up()
    sys.exit(-1)


def run_cmd(cmd):
    print(cmd)
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).decode("utf-8")
    else:
        return subprocess.check_output(cmd.split(" ")).decode("utf-8")


def continue_maybe(prompt):
    result = input("\n%s (y/n): " % prompt)
    if result.lower() != "y":
        fail("Okay, exiting")


def clean_up():
    if "original_head" in globals():
        print("Restoring head pointer to %s" % original_head)
        run_cmd("git checkout %s" % original_head)

        branches = run_cmd("git branch").replace(" ", "").split("\n")

        for branch in list(filter(lambda x: x.startswith(BRANCH_PREFIX), branches)):
            print("Deleting local branch %s" % branch)
            run_cmd("git branch -D %s" % branch)


# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref, title, body, pr_repo_desc):
    pr_branch_name = "%s_MERGE_PR_%s" % (BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (BRANCH_PREFIX, pr_num, target_ref.upper())
    run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        run_cmd(["git", "merge", pr_branch_name, "--squash"])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    # First commit author should be considered as the primary author when the rank is the same
    commit_authors = run_cmd(
        ["git", "log", "HEAD..%s" % pr_branch_name, "--pretty=format:%an <%ae>", "--reverse"]
    ).split("\n")
    distinct_authors = sorted(
        list(dict.fromkeys(commit_authors)), key=lambda x: commit_authors.count(x), reverse=True
    )
    primary_author = input(
        'Enter primary author in the format of "name <email>" [%s]: ' % distinct_authors[0]
    )
    if primary_author == "":
        primary_author = distinct_authors[0]
    else:
        # When primary author is specified manually, de-dup it from author list and
        # put it at the head of author list.
        distinct_authors = list(filter(lambda x: x != primary_author, distinct_authors))
        distinct_authors.insert(0, primary_author)

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    if body is not None:
        # We remove @ symbols from the body to avoid triggering e-mails
        # to people every time someone creates a public fork of Celeborn.
        merge_message_flags += ["-m", body.replace("@", "")]

    committer_name = run_cmd("git config --get user.name").strip()
    committer_email = run_cmd("git config --get user.email").strip()

    if had_conflicts:
        message = "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>" % (
            committer_name,
            committer_email,
        )
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    merge_message_flags += ["-m", "Closes #%s from %s." % (pr_num, pr_repo_desc)]

    authors = "Authored-by:" if len(distinct_authors) == 1 else "Lead-authored-by:"
    authors += " %s" % (distinct_authors.pop(0))
    if len(distinct_authors) > 0:
        authors += "\n" + "\n".join(["Co-authored-by: %s" % a for a in distinct_authors])
    authors += "\n" + "Signed-off-by: %s <%s>" % (committer_name, committer_email)

    merge_message_flags += ["-m", authors]

    run_cmd(["git", "commit", '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe(
        "Merge complete (local ref %s). Push to %s?" % (target_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        run_cmd("git push %s %s:%s" % (PUSH_REMOTE_NAME, target_branch_name, target_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash


def cherry_pick(pr_num, merge_hash, default_branch):
    pick_ref = input("Enter a branch name [%s]: " % default_branch)
    if pick_ref == "":
        pick_ref = default_branch

    pick_branch_name = "%s_PICK_PR_%s_%s" % (BRANCH_PREFIX, pr_num, pick_ref.upper())

    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
    run_cmd("git checkout %s" % pick_branch_name)

    try:
        run_cmd("git cherry-pick -sx %s" % merge_hash)
    except Exception as e:
        msg = "Error cherry-picking: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and finish the cherry-pick. Finished?"
        continue_maybe(msg)

    continue_maybe(
        "Pick complete (local ref %s). Push to %s?" % (pick_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        run_cmd("git push %s %s:%s" % (PUSH_REMOTE_NAME, pick_branch_name, pick_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    pick_hash = run_cmd("git rev-parse %s" % pick_branch_name)[:8]
    clean_up()

    print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
    print("Pick hash: %s" % pick_hash)
    return pick_ref


def resolve_jira_issue(merge_branches, comment, default_jira_id=""):
    jira_id = input("Enter a JIRA id [%s]: " % default_jira_id)
    if jira_id == "":
        jira_id = default_jira_id

    try:
        issue = asf_jira.issue(jira_id)
    except Exception as e:
        fail("ASF JIRA could not find %s\n%s" % (jira_id, e))

    cur_status = issue.fields.status.name
    cur_summary = issue.fields.summary
    cur_assignee = issue.fields.assignee
    if cur_assignee is None:
        cur_assignee = choose_jira_assignee(issue)
    # Check again, we might not have chosen an assignee
    if cur_assignee is None:
        cur_assignee = "NOT ASSIGNED!!!"
    else:
        cur_assignee = cur_assignee.displayName

    if cur_status == "Resolved" or cur_status == "Closed":
        fail("JIRA issue %s already has status '%s'" % (jira_id, cur_status))
    print("=== JIRA %s ===" % jira_id)
    print(
        "summary\t\t%s\nassignee\t%s\nstatus\t\t%s\nurl\t\t%s/%s\n"
        % (cur_summary, cur_assignee, cur_status, JIRA_BASE, jira_id)
    )

    versions = asf_jira.project_versions("CELEBORN")
    # Consider only x.y.z, unreleased, unarchived versions
    versions = [
        x
        for x in versions
        if not x.raw["released"] and not x.raw["archived"] and re.match(r"\d+\.\d+\.\d+", x.name)
    ]

    default_fix_versions = []
    for b in merge_branches:
        if b == "main":
            default_fix_versions.append(versions[0].name)
        else:
            found = False
            found_versions = []
            for v in versions:
                if v.name.startswith(b.replace("branch-", "")):
                    found_versions.append(v.name)
                    found = True
            if found:
                # There might be several unreleased versions for specific branches
                # For example, assuming
                # versions = ['4.0.0', '3.5.1', '3.5.0', '3.4.2', '3.3.4', '3.3.3']
                # we've found two candidates for branch-3.5, we pick the last/smallest one
                default_fix_versions.append(found_versions[-1])
            else:
                print(
                    "Target version for %s is not found on JIRA, it may be archived or "
                    "not created. Skipping it." % b
                )

    for v in default_fix_versions:
        # Handles the case where we have forked a release branch but not yet made the release.
        # In this case, if the PR is committed to the main branch and the release branch, we
        # only consider the release branch to be the fix version. E.g. it is not valid to have
        # both 1.1.0 and 1.0.0 as fix versions.
        (major, minor, patch) = v.split(".")
        if patch == "0":
            previous = "%s.%s.%s" % (major, int(minor) - 1, 0)
            if previous in default_fix_versions:
                default_fix_versions = list(filter(lambda x: x != v, default_fix_versions))
    default_fix_versions = ",".join(default_fix_versions)

    available_versions = set(list(map(lambda v: v.name, versions)))
    while True:
        try:
            fix_versions = input(
                "Enter comma-separated fix version(s) [%s]: " % default_fix_versions
            )
            if fix_versions == "":
                fix_versions = default_fix_versions
            fix_versions = fix_versions.replace(" ", "").split(",")
            if set(fix_versions).issubset(available_versions):
                break
            else:
                print(
                    "Specified version(s) [%s] not found in the available versions, try "
                    "again (or leave blank and fix manually)." % (", ".join(fix_versions))
                )
        except KeyboardInterrupt:
            raise
        except BaseException:
            traceback.print_exc()
            print("Error setting fix version(s), try again (or leave blank and fix manually)")

    def get_version_json(version_str):
        return list(filter(lambda v: v.name == version_str, versions))[0].raw

    jira_fix_versions = list(map(lambda v: get_version_json(v), fix_versions))

    resolve = list(filter(lambda a: a["name"] == "Resolve Issue", asf_jira.transitions(jira_id)))[0]
    resolution = list(filter(lambda r: r.raw["name"] == "Fixed", asf_jira.resolutions()))[0]
    asf_jira.transition_issue(
        jira_id,
        resolve["id"],
        fixVersions=jira_fix_versions,
        comment=comment,
        resolution={"id": resolution.raw["id"]},
    )

    print("Successfully resolved %s with fixVersions=%s!" % (jira_id, fix_versions))


def choose_jira_assignee(issue):
    """
    Prompt the user to choose who to assign the issue to in jira, given a list of candidates,
    including the original reporter and all commentors
    """
    while True:
        try:
            reporter = issue.fields.reporter
            commentors = list(map(lambda x: x.author, issue.fields.comment.comments))
            candidates = set(commentors)
            candidates.add(reporter)
            candidates = list(candidates)
            print("JIRA is unassigned, choose assignee")
            for idx, author in enumerate(candidates):
                if "celeborn" in author.key:
                    continue
                annotations = ["Reporter"] if author == reporter else []
                if author in commentors:
                    annotations.append("Commentor")
                print("[%d] %s (%s)" % (idx, author.displayName, ",".join(annotations)))
            raw_assignee = input(
                "Enter number of user, or userid, to assign to (blank to leave unassigned):"
            )
            if raw_assignee == "":
                return None
            else:
                try:
                    id = int(raw_assignee)
                    assignee = candidates[id]
                except BaseException:
                    # assume it's a user id, and try to assign (might fail, we just prompt again)
                    assignee = asf_jira.user(raw_assignee)
                try:
                    assign_issue(issue.key, assignee.name)
                except Exception as e:
                    if (
                        e.__class__.__name__ == "JIRAError"
                        and ("'%s' cannot be assigned" % assignee.name)
                        in getattr(e, "response").text
                    ):
                        continue_maybe(
                            "User '%s' cannot be assigned, add to contributors role and try again?"
                            % assignee.name
                        )
                        grant_contributor_role(assignee.name)
                        assign_issue(issue.key, assignee.name)
                    else:
                        raise e
                return assignee
        except KeyboardInterrupt:
            raise
        except BaseException:
            traceback.print_exc()
            print("Error assigning JIRA, try again (or leave blank and fix manually)")

def grant_contributor_role(user: str):
    role = asf_jira.project_role("CELEBORN", 10010)
    role.add_user(user)
    print("Successfully added user '%s' to contributors role" % user)

def assign_issue(issue: int, assignee: str) -> bool:
    """
    Assign an issue to a user, which is a shorthand for jira.client.JIRA.assign_issue.
    The original one has an issue that it will search users again and only choose the assignee
    from 20 candidates. If it's unmatched, it picks the head blindly. In our case, the assignee
    is already resolved.
    """
    url = getattr(asf_jira, "_get_latest_url")(f"issue/{issue}/assignee")
    payload = {"name": assignee}
    getattr(asf_jira, "_session").put(url, data=json.dumps(payload))
    return True



def resolve_jira_issues(title, merge_branches, comment):
    jira_ids = re.findall("CELEBORN-[0-9]{3,6}", title)

    if len(jira_ids) == 0:
        resolve_jira_issue(merge_branches, comment)
    for jira_id in jira_ids:
        resolve_jira_issue(merge_branches, comment, jira_id)


def standardize_jira_ref(text):
    """
    Standardize the [CELEBORN-XXX][MODULE] prefix
    Converts "[CELEBORN-XXX][core] Issue", "[Core] CELEBORN-XXX. Issue" or "CELEBORN XXX [CORE]: Issue" to
    "[CELEBORN-XXX][CORE] Issue"
    """
    jira_refs = []
    components = []

    # If the string is compliant, no need to process any further
    if re.search(r"^\[CELEBORN-[0-9]{3,6}\](\[[A-Z0-9_\s,]+\] )+\S+", text):
        return text

    # Extract JIRA ref(s):
    pattern = re.compile(r"(CELEBORN[-\s]*[0-9]{3,6})+", re.IGNORECASE)
    for ref in pattern.findall(text):
        # Add brackets, replace spaces with a dash, & convert to uppercase
        jira_refs.append("[" + re.sub(r"\s+", "-", ref.upper()) + "]")
        text = text.replace(ref, "")

    # Extract celeborn component(s):
    # Look for alphanumeric chars, spaces, dashes, periods, and/or commas
    pattern = re.compile(r"(\[[\w\s,.-]+\])", re.IGNORECASE)
    for component in pattern.findall(text):
        components.append(component.upper())
        text = text.replace(component, "")

    # Cleanup any remaining symbols:
    pattern = re.compile(r"^\W+(.*)", re.IGNORECASE)
    if pattern.search(text) is not None:
        text = pattern.search(text).groups()[0]

    # Assemble full text (JIRA ref(s), module(s), remaining text)
    clean_text = "".join(jira_refs).strip() + "".join(components).strip() + " " + text.strip()

    # Replace multiple spaces with a single space, e.g. if no jira refs and/or components were
    # included
    clean_text = re.sub(r"\s+", " ", clean_text.strip())

    return clean_text


def get_current_ref():
    ref = run_cmd("git rev-parse --abbrev-ref HEAD").strip()
    if ref == "HEAD":
        # The current ref is a detached HEAD, so grab its SHA.
        return run_cmd("git rev-parse HEAD").strip()
    else:
        return ref


def initialize_jira():
    global asf_jira
    jira_server = {"server": JIRA_API_BASE}

    if not JIRA_IMPORTED:
        print("ERROR finding jira library. Run 'pip3 install jira' to install.")
        continue_maybe("Continue without jira?")
    elif JIRA_ACCESS_TOKEN:
        client = jira.client.JIRA(jira_server, token_auth=JIRA_ACCESS_TOKEN)
        try:
            # Eagerly check if the token is valid to align with the behavior of username/password
            # authn
            client.current_user()
            asf_jira = client
        except Exception as e:
            if e.__class__.__name__ == "JIRAError" and getattr(e, "status_code", None) == 401:
                msg = (
                        "ASF JIRA could not authenticate with the invalid or expired token '%s'"
                        % JIRA_ACCESS_TOKEN
                )
                fail(msg)
            else:
                raise e
    elif ASF_USERNAME and ASF_PASSWORD:
        asf_jira = jira.client.JIRA(jira_server, basic_auth=(ASF_USERNAME, ASF_PASSWORD))
    else:
        print("Neither JIRA_ACCESS_TOKEN nor ASF_USERNAME/ASF_PASSWORD are set.")
        continue_maybe("Continue without jira?")


def main():
    initialize_jira()
    global original_head

    os.chdir(PROJECT_HOME)
    original_head = get_current_ref()

    branches = get_json("%s/branches" % GITHUB_API_BASE)
    branch_names = list(filter(lambda x: x.startswith("branch-"), [x["name"] for x in branches]))
    # Assumes branch names can be sorted lexicographically
    branch_names = sorted(branch_names, reverse=True)
    branch_iter = iter(branch_names)

    pr_num = input("Which pull request would you like to merge? (e.g. 34): ")
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))

    url = pr["url"]

    # Warn if the PR is WIP
    if "[WIP]" in pr["title"]:
        msg = "The PR title has `[WIP]`:\n%s\nContinue?" % pr["title"]
        continue_maybe(msg)

    # Decide whether to use the modified title or not
    modified_title = standardize_jira_ref(pr["title"]).rstrip(".")
    if modified_title != pr["title"]:
        print("I've re-written the title as follows to match the standard format:")
        print("Original: %s" % pr["title"])
        print("Modified: %s" % modified_title)
        result = input("Would you like to use the modified title? (y/n): ")
        if result.lower() == "y":
            title = modified_title
            print("Using modified title:")
        else:
            title = pr["title"]
            print("Using original title:")
        print(title)
    else:
        title = pr["title"]

    body = pr["body"]
    if body is None:
        body = ""
    modified_body = re.sub(re.compile(r"<!--[^>]*-->\n?", re.DOTALL), "", body).lstrip()
    if modified_body != body:
        print("=" * 80)
        print(modified_body)
        print("=" * 80)
        print("I've removed the comments from PR template like the above:")
        result = input("Would you like to use the modified body? (y/n): ")
        if result.lower() == "y":
            body = modified_body
            print("Using modified body:")
        else:
            print("Using original body:")
        print("=" * 80)
        print(body)
        print("=" * 80)
    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)

    # Merged pull requests don't appear as merged in the GitHub API;
    # Instead, they're closed by committers.
    merge_commits = [
        e for e in pr_events if e["event"] == "closed" and e["commit_id"] is not None
    ]

    if merge_commits:
        merge_hash = merge_commits[0]["commit_id"]
        message = get_json("%s/commits/%s" % (GITHUB_API_BASE, merge_hash))["commit"]["message"]

        print("Pull request %s has already been merged, assuming you want to backport" % pr_num)
        commit_is_downloaded = (
            run_cmd(["git", "rev-parse", "--quiet", "--verify", "%s^{commit}" % merge_hash]).strip()
            != ""
        )
        if not commit_is_downloaded:
            fail("Couldn't find any merge commit for #%s, you may need to update HEAD." % pr_num)

        print("Found commit %s:\n%s" % (merge_hash, message))
        cherry_pick(pr_num, merge_hash, next(branch_iter, branch_names[0]))
        sys.exit(0)

    if not bool(pr["mergeable"]):
        msg = (
            "Pull request %s is not mergeable in its current form.\n" % pr_num
            + "Continue? (experts only!)"
        )
        continue_maybe(msg)

    print("\n=== Pull Request #%s ===" % pr_num)
    print("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s" % (title, pr_repo_desc, target_ref, url))
    continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    merged_refs = [target_ref]

    merge_hash = merge_pr(pr_num, target_ref, title, body, pr_repo_desc)

    pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
    while input("\n%s (y/n): " % pick_prompt).lower() == "y":
        merged_refs = merged_refs + [
            cherry_pick(pr_num, merge_hash, next(branch_iter, branch_names[0]))
        ]

    if asf_jira is not None:
        continue_maybe("Would you like to update an associated JIRA?")
        jira_comment = "Issue resolved by pull request %s\n[%s/%s]" % (
            pr_num,
            GITHUB_BASE,
            pr_num,
        )
        resolve_jira_issues(title, merged_refs, jira_comment)
    else:
        print("Exiting without trying to close the associated JIRA.")


if __name__ == "__main__":
    import doctest

    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
    try:
        main()
    except BaseException:
        clean_up()
        raise
