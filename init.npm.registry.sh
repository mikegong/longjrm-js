#!/bin/sh
npm set strict-ssl=false
npm config set registry https://af.cds.bns/artifactory/api/npm/virtual-npm-bns/
npm login