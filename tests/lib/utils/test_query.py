# Copyright 2016 Mirantis Inc.
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
import mock

from cloudferry import model
from cloudferry.lib.utils import query
from tests.lib.utils import test_local_db


class TestMode(model.Model):
    object_id = model.PrimaryKey()
    field1 = model.String()
    field2 = model.String()


CLASS_FQN = TestMode.__module__ + '.' + TestMode.__name__


class StageTestCase(test_local_db.DatabaseMockingTestCase):
    @staticmethod
    def _make_id(model_class, uuid, cloud='test_cloud'):
        return {
            'id': uuid,
            'cloud': cloud,
            'type': model_class.get_class_qualname(),
        }

    def setUp(self):
        super(StageTestCase, self).setUp()

        self.cloud = mock.MagicMock()
        self.cloud.name = 'test_cloud'

        self.obj1 = TestMode.load({
            'object_id': self._make_id(TestMode, 'id1'),
            'field1': 'a',
            'field2': 'a',
        })
        self.obj2 = TestMode.load({
            'object_id': self._make_id(TestMode, 'id2'),
            'field1': 'a',
            'field2': 'b',
        })
        self.obj3 = TestMode.load({
            'object_id': self._make_id(TestMode, 'id3'),
            'field1': 'b',
            'field2': 'a',
        })
        self.obj4 = TestMode.load({
            'object_id': self._make_id(TestMode, 'id4'),
            'field1': 'b',
            'field2': 'b',
        })

        with model.Session() as s:
            s.store(self.obj1)
            s.store(self.obj2)
            s.store(self.obj3)
            s.store(self.obj4)

    def test_simple_query1(self):
        q = query.Query({
            CLASS_FQN: [
                {
                    'field1': ['a'],
                }
            ]
        })
        with model.Session() as session:
            objs = sorted(q.search(session), key=lambda x: x.object_id.id)
            self.assertEqual(2, len(objs))
            self.assertEqual(objs[0].object_id.id, 'id1')
            self.assertEqual(objs[1].object_id.id, 'id2')

    def test_simple_query2(self):
        q = query.Query({
            CLASS_FQN: [
                {
                    'field1': ['b'],
                    'field2': ['b'],
                }
            ]
        })
        with model.Session() as session:
            objs = sorted(q.search(session), key=lambda x: x.object_id.id)
            self.assertEqual(1, len(objs))
            self.assertEqual(objs[0].object_id.id, 'id4')

    def test_simple_query3(self):
        q = query.Query({
            CLASS_FQN: [
                {
                    'field1': ['a'],
                },
                {
                    'field2': ['b'],
                },
            ]
        })
        with model.Session() as session:
            objs = sorted(q.search(session), key=lambda x: x.object_id.id)
            self.assertEqual(3, len(objs))
            self.assertEqual(objs[0].object_id.id, 'id1')
            self.assertEqual(objs[1].object_id.id, 'id2')
            self.assertEqual(objs[2].object_id.id, 'id4')

    def test_simple_query_negative(self):
        q = query.Query({
            CLASS_FQN: [
                {
                    '!field1': ['b'],
                    'field2': ['b'],
                }
            ]
        })
        with model.Session() as session:
            objs = sorted(q.search(session), key=lambda x: x.object_id.id)
            self.assertEqual(1, len(objs))
            self.assertEqual(objs[0].object_id.id, 'id2')

    def test_jmespath_query(self):
        q = query.Query({
            CLASS_FQN: [
                '[? field1 == `b` && field2 == `a` ]'
            ]
        })
        with model.Session() as session:
            objs = sorted(q.search(session), key=lambda x: x.object_id.id)
            self.assertEqual(1, len(objs))
            self.assertEqual(objs[0].object_id.id, 'id3')
