# Copyright 2016 Quora, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import qcore
import inspect
import six
from six.moves import xrange
from asynq import async, async_proxy, ConstFuture, result, FutureBase


unknown = qcore.MarkerObject(u'unknown @ entities')
must_load = qcore.MarkerObject(u'must_load @ entities')


class EntityState(qcore.Flags):
    # Base flags
    new = 1
    deleted = 2
    loaded = 0x10
    changed = 0x20
    # Few useful combinations
    must_load = 0
    new_must_save = new | loaded | changed
    deleted_saved = loaded | deleted
    deleted_must_save = loaded | deleted | changed

    # A few useful check methods

    def is_new(self):
        return EntityState.new in self

    def is_deleted(self):
        return EntityState.deleted in self

    def is_loaded(self):
        return EntityState.loaded in self

    def is_changed(self):
        return EntityState.changed in self

    def is_synchronized(self):
        return \
            EntityState.loaded in self and \
            EntityState.changed not in self


class PersisterBase(object):
    @async_proxy(pure=True)
    def sync(self, entity):
        # print 'Syncing: %r' % entity
        state = entity.state
        if not state.is_loaded():
            assert not state.is_changed(), \
                'Cannot sync: the entity is not yet loaded, but changed.'
            task = self.load(entity)
            task.on_computed.subscribe(lambda _: entity._mark_synchronized())
            return task
        else:
            return self.save(entity)

    @async_proxy(pure=True)
    def save(self, entity):
        state = entity.state
        if not state.is_changed():
            return ConstFuture(None)
        if state.is_new():
            task = self.create(entity)
        elif state.is_deleted():
            task = self.delete(entity)
        else:
            task = self.update(entity)
        task.on_computed.subscribe(lambda _: entity._mark_synchronized())
        return task

    @async(pure=True)
    def create(self, entity):
        # Must raise an exception if entity cannot be created
        # (e.g. if it already exists in the storage)
        raise NotImplementedError()

    @async(pure=True)
    def load(self, entity):
        # Must raise an exception if entity cannot be loaded
        # (e.g. if it absents in the storage)
        raise NotImplementedError()

    @async(pure=True)
    def update(self, entity):
        # Must raise an exception if entity cannot be saved
        # (e.g. if it absents in the storage)
        raise NotImplementedError()

    @async(pure=True)
    def delete(self, entity):
        # Must raise an exception if entity cannot be deleted
        # (e.g. if it absents in the storage)
        raise NotImplementedError()


class NotImplementedPersister(PersisterBase):
    pass


class NoPersister(PersisterBase):
    @async(pure=True)
    def create(self, entity):
        entity._mark_synchronized()

    @async(pure=True)
    def load(self, entity):
        entity._mark_synchronized()

    @async(pure=True)
    def update(self, entity):
        entity._mark_synchronized()

    @async(pure=True)
    def delete(self, entity):
        entity._mark_synchronized()


class EntityField(object):
    cls = None
    name = None
    internal_name = None
    backing_name = None
    is_key = False
    key_index = None
    is_lazy = False

    def __init__(self, default=None, name=None, backing_name=None):
        self.default = default
        self.set_name(name, backing_name=backing_name)

    def set_name(self, name, backing_name=None):
        self.name = name
        self.internal_name = None if name is None else '_p_' + name
        self.backing_name = backing_name or name
        return self

    def key(self, index=0):
        self.is_key = index is not None
        self.key_index = index
        return self

    def lazy(self, lazy=True):
        self.is_lazy = lazy
        return self

    def is_own(self, cls):
        return self.cls is cls

    def is_inherited(self, cls):
        return self.cls is not cls

    def __get__(self, instance, owner):
        if instance is None:
            return self  # Handles cls.property case
        result = instance.__dict__.get(self.name, unknown)
        if result is unknown and instance.state.is_new():
            return self.default
        return result

    def __set__(self, instance, value):
        assert not self.is_key, "Key field %r cannot be changed." % self.name
        if instance.track_changes:
            d = instance.__dict__
            old_value = instance.__dict__.get(self.name, unknown)
            if old_value is unknown and instance.state.is_new():
                old_value = self.default
            d[self.name] = value
            if old_value != value:
                instance._mark_changed()
                if 'changes' in d:
                    instance.changes.add(self.name)
                else:
                    instance.changes = set((self.name,))
        else:
            instance.__dict__[self.name] = value
            instance._mark_changed()

    def set_unsafe(self, instance, value):
        instance.__dict__[self.name] = value

    def __repr__(self):
        result = "%s(%r, name=%r" % (
            self.__class__.__name__, self.default, self.name)
        if self.backing_name != self.name:
            result += ', backing_name=%r' % self.backing_name
        result += ')'
        if self.is_key:
            result += '.key(%r)' % self.key_index
        if self.is_lazy:
            result += '.lazy()'
        return result


class EntityMetadata(object):
    def __init__(self, cls):
        self.cls = cls
        self.all_fields = []
        self.key_fields = []
        self.non_key_fields = []
        self.fields = {}
        self.field_indexes = {}

        for name, f in list(inspect.getmembers(cls)):
            if not isinstance(f, EntityField):
                continue
            if f.cls is None:
                f.cls = cls
            if f.name is None:
                f.set_name(name)
            else:
                assert name == f.name, \
                    'EntityField name mismatch: %r != %r (class: %s).' % \
                    (name, f.name, qcore.inspection.get_full_name(cls))

            assert name not in self.fields, \
                'Duplicate property name: %s (class: %s).' % \
                (name, qcore.inspection.get_full_name(cls))
            self.fields[f.name] = f

            if f.is_key:
                self.key_fields.append(f)
            else:
                self.non_key_fields.append(f)

        self.key_fields = sorted(self.key_fields, key=lambda f: f.key_index)

        for i in xrange(0, len(self.key_fields)):
            f = self.key_fields[i]
            assert i == f.key_index, \
                'Key index mismatch for property %s (class: %s): %r != %r.' % \
                (f.name, qcore.inspection.get_full_name(cls), i, f.key_index)
            assert not f.is_lazy, \
                'Key property cannot be lazy: %s (class: %s).' % \
                (f.name, qcore.inspection.get_full_name(cls))

        self.all_fields = self.key_fields + self.non_key_fields
        for i in xrange(0, len(self.all_fields)):
            f = self.all_fields[i]
            self.field_indexes[f.name] = i

    def __repr__(self):
        def list_fields(fields):
            result = ''
            for f in fields:
                result += '\n    %r' % f
                if f.is_inherited(self.cls):
                    result += ' # Inherited'
            return result

        result = '%s(%s):' % (
            self.__class__.__name__,
            qcore.inspection.get_full_name(self.cls))
        if self.key_fields:
            result += '\n  Key fields:'
            result += list_fields(self.key_fields)
            result += '\n  Other fields:'
            result += list_fields(self.non_key_fields)
        return result


class EntityType(type):
    def __init__(cls, what, bases=None, dict=None):
        super(EntityType, cls).__init__(what, bases, dict)
        cls.metadata = EntityMetadata(cls)


class Entity(six.with_metaclass(EntityType, FutureBase)):
    persister = NotImplementedPersister()
    metadata = None  # Set by metaclass

    changes = None
    track_changes = False

    def __init__(self, key, direct_call=True):
        super(Entity, self).__init__()
        key_fields = self.metadata.key_fields
        key_field_count = len(key_fields)
        assert direct_call is False, \
            "Entity.__init__ cannot be called directly. " \
            "Use Entity.get(...) or Entity.new(...) instead."
        assert key_field_count > 0, \
            "Key is not defined for %s." % \
            qcore.inspection.get_full_name(self.__class__)
        assert key_field_count == len(key), \
            "Key length (%i) differ from the expected one (%i)." % (
                len(key), key_field_count)
        self._state = EntityState.must_load
        for i in xrange(0, key_field_count):
            f = key_fields[i]
            v = key[i]
            assert not (v is None or v is unknown), \
                "Key field %r cannot be %r." % (f.name, v)
            f.set_unsafe(self, v)

    @classmethod
    def get(cls, *key):
        return cls(key, direct_call=False)

    @classmethod
    def new(cls, *key):
        return cls(key, direct_call=False)._mark_new()

    @property
    def state(self):
        return self._state

    @property
    def key(self):
        return tuple([
            getattr(self, f.name)
            for f in self.metadata.key_fields
        ])

    @property
    def full_key(self):
        return (type(self),) + self.key

    def update(self, **values):
        """A helper method allowing to set multiple fields at once."""
        for k, v in values.items():
            setattr(self, k, v)
        return self

    def delete(self):
        return self._mark_deleted()

    def get_data(self, changes_only=False):
        data = {}
        if changes_only:
            if not self.state.is_changed():
                return data
            if not self.track_changes:
                changes_only = False
        if changes_only:
            changes = self.changes or qcore.empty_tuple
            for f in self.metadata.non_key_fields:
                v = getattr(self, f.name)
                if v is not unknown and f.name in changes:
                    data[f.name] = v
        else:
            for f in self.metadata.non_key_fields:
                v = getattr(self, f.name)
                if v is not unknown:
                    data[f.name] = v
        return data

    def update_data(self, data, mark_changed=True):
        if mark_changed:
            self._mark_changed()
        fields = self.metadata.fields
        for k, v in data.items():
            f = fields.get(k)
            if f is None or f.is_key:
                continue
            f.set_unsafe(self, v)

    @async(pure=True)
    def sync(self):
        ret = yield self.persister.sync(self)
        result(ret); return

    def _compute(self):
        if not self.is_computed():
            self.set_value(self)  # Prevents the recursion
        return self.sync()()

    def _mark_new(self):
        self._state = EntityState.new_must_save
        if 'changes' in self.__dict__:
            del self.changes
        self.reset_unsafe()
        return self

    def _mark_changed(self):
        assert self._state.is_loaded(), "The entity must be loaded first."
        self._state |= EntityState.changed
        self.reset_unsafe()
        return self

    def _mark_deleted(self):
        assert self._state.is_loaded(), "The entity must be loaded first."
        self._state |= EntityState.deleted_must_save
        self.reset_unsafe()
        return self

    def _mark_synchronized(self):
        if self._state.is_deleted():
            self._state = self.state.deleted_saved
        else:
            self._state = self.state.loaded
        if 'changes' in self.__dict__:
            del self.changes
        return self

    def __getstate(self):
        return self.__dict__

    def __setstate__(self, state):
        # Needed to properly unpickle entities
        self.__dict__ = state
        if self._state.is_changed():
            self.reset_unsafe()

    def __repr__(self):
        result = '%s(%s):' % (
            self.__class__.__name__,
            ', '.join([repr(f) for f in self.key]))
        result += '\n  State: %s' % self.state
        if self.track_changes and self.changes:
            result += ' (%s)' % ', '.join(self.changes)
        for k, v in self.get_data().items():
            result += '\n  %s: %r' % (k, v)
        return result
