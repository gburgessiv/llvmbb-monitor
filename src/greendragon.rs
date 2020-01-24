use crate::Bot;
use crate::BotID;
use crate::BotStatus;
use crate::BotStatusSnapshot;
use crate::BuildNumber;
use crate::BuildbotResult;
use crate::BuilderState;
use crate::CompletedBuild;
use crate::Email;
use crate::FailureOr;
use crate::Master;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::Fuse;
use std::time::Duration;

use failure::bail;
use lazy_static::lazy_static;
use log::{debug, error, warn};
use serde::Deserialize;

#[derive(Deserialize)]
struct UnabridgedBuildStatus;

#[derive(Deserialize)]
struct UnabridgedBuilderStatus;

// !! FIXME: greendragon notes !!
// fhahn notes
// """
// @gburgessiv It looks like green.lab.llvm.org has a REST API link at the bottom of the pages for
// the bots (and the main index), like
// http://green.lab.llvm.org/green/job/clang-stage1-cmake-RA-incremental/api/ which describes the
// endpoints :slight_smile:
// """
//
// http://green.lab.llvm.org/green/api/json is probably the main endpoint; it has things like
// clang-stage1-cmake-RA-expensive.
//
// An example of a single builder's recent builds is:
// http://green.lab.llvm.org/green/job/clang-stage1-cmake-RA-incremental/api/json?pretty=true
//
// http://green.lab.llvm.org/green/job/clang-stage1-cmake-RA-incremental/8387/api/json?pretty=true
// is a single build (so just add /api/json?pretty=true to a link)
//
// OK. So `changeSets` is left empty if it's an incomplete build, and populated if it's complete.
// That's something to go off of, at least. They also have `authorEmail` fields, so we can pretty
// easily turn that into a blamelist. Notably, authorEmail excludes the name of the author.
//
// authorEmail is once per commit in changeSets. No author name is there. If we want to map to
// author name, we have commitId; we can maintain a llvm-project checkout and go from there, but
// that's probably unnecessary.
//
// RE the build status type, we have a 'result'; "FAILURE" is one value for this. Need to figure
// that out more deeply.
//
// In any case, the question I have at this point is "if we were to start fresh, how would I change
// what's here?"
//
// BotStatusSnapshot is probably still appropriate, as is Bot (GreenDragon can maybe be the
// category? Just have to hope the two don't collide.)
// BotStatus works.
// state works.
// completedbuild works.
//
// cool. so just make them not collide by adding a `master` bit to names.
//
// I have lotsa URLs to tweak though. Let's start with that.

lazy_static! {
    static ref HOST: reqwest::Url =
        reqwest::Url::parse("http://green.lab.llvm.org").expect("parsing greendragon URL");
}

pub(crate) async fn fetch_new_status_snapshot(
    client: &reqwest::Client,
    prev: &HashMap<String, Bot>,
) -> FailureOr<HashMap<String, Bot>> {
    Ok(HashMap::new())
}

async fn json_get<T>(client: &reqwest::Client, path: &str) -> FailureOr<T>
where
    T: serde::de::DeserializeOwned,
{
    let resp = client.get(HOST.join(path)?).send().await?;
    Ok(resp.json().await?)
}
