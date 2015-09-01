# -*- encoding: utf-8 -*-
# Copyright 2015 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from mrjob.logparsers import EMR_JOB_LOG_URI_RE


from tests.py2 import TestCase


class URIRegexTestCase(TestCase):

    def test_emr_job_re_on_2_x_ami(self):
        uri = 'ssh://ec2-52-88-7-250.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/history/done/version-1/ip-172-31-29-201.us-west-2.compute.internal_1441062912502_/2015/08/31/000000/job_201508312315_0001_1441062985499_hadoop_streamjob1474198573915234945.jar'
        m = EMR_JOB_LOG_URI_RE.match(uri)
        self.assertTrue(m)
        self.assertEqual(m.group('step_num'), '0001')

    def test_emr_job_re_on_3_x_ami(self):
        uri = 'ssh://ec2-52-24-131-73.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/history/2015/08/31/000000/job_1441057410014_0001-1441057493406-hadoop-streamjob6928722756977481487.jar-1441057604210-2-1-SUCCEEDED-default-1441057523674.jhist'  # noqa
        m = EMR_JOB_LOG_URI_RE.match(uri)
        self.assertTrue(m)
        self.assertEqual(m.group('step_num'), '0001')
