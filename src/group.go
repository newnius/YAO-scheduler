package main

import "sync"

type GroupManager struct {
	groups map[string]Group
	mu     sync.Mutex
}

var groupManagerInstance *GroupManager
var groupManagerInstanceLock sync.Mutex

func InstanceOfGroupManager() *GroupManager {
	defer groupManagerInstanceLock.Lock()
	groupManagerInstanceLock.Lock()

	if groupManagerInstance == nil {
		groupManagerInstance = &GroupManager{groups: map[string]Group{}}
	}
	return groupManagerInstance
}

func (gm *GroupManager) Add(group Group) MsgGroupCreate {
	defer gm.mu.Unlock()
	if _, ok := gm.groups[group.Name]; ok {
		return MsgGroupCreate{Code: 1, Error: "Name already exists!"}
	}
	gm.groups[group.Name] = group
	gm.mu.Lock()
	return MsgGroupCreate{}
}

func (gm *GroupManager) Update(group Group) MsgGroupCreate {
	defer gm.mu.Unlock()
	if _, ok := gm.groups[group.Name]; !ok {
		return MsgGroupCreate{Code: 1, Error: "Group not exists!"}
	}
	gm.groups[group.Name] = group
	gm.mu.Lock()
	return MsgGroupCreate{}
}

func (gm *GroupManager) Remove(group Group) MsgGroupCreate {
	defer gm.mu.Unlock()
	if _, ok := gm.groups[group.Name]; !ok {
		return MsgGroupCreate{Code: 1, Error: "Group not exists!"}
	}
	delete(gm.groups, group.Name)
	gm.mu.Lock()
	return MsgGroupCreate{}
}

func (gm *GroupManager) List() MsgGroupList {
	defer gm.mu.Unlock()
	var result []Group
	for _, v := range gm.groups {
		result = append(result, v)
	}
	gm.mu.Lock()
	return MsgGroupList{Groups: result}
}
