var util = require('util');
var fs = require('fs');

var racks = {
    'sjc1-1': 20,
    'sjc1-2': 2,
    'sjc1-3': 3,
    'sjc1-5': 5,
    'sjc1-8': 8,
};

var hosts = createHosts(racks);

var shards = 128;
var replicas = 3;
var assignment = createAssignment(hosts, shards, replicas);

console.log('assignment:\n' + inspect(assignment) + '\n');
if (process.env.hasOwnProperty('WRITE_FILE')) {
    var file = process.env.WRITE_FILE;
    fs.writeFileSync(file, JSON.stringify(assignment, null, 4), 'utf8');
}

function createAssignment(hosts, shards, replicas) {
    var replicaSets = [];
    for (var i = 0; i < replicas; i++) {
        var replicaSet = {
            replica: i,
            shards: [],
        };
        for (var j = 0; j < shards; j++) {
            replicaSet.shards.push({
                replica: i,
                index: j,
                host: null
            });
        }
        replicaSets.push(replicaSet);
    }

    var unassignedReplicaSets = [];
    for (var i = 0; i < replicas; i++) {
        var replicaSet = {
            replica: i,
            shards: replicaSets[i].shards.concat([]), // TODO: rotate each by shards/replicas to avoid mirrors
        };
        unassignedReplicaSets.push(replicaSet);
    }

    var racksByName = {};
    hosts.forEach(function eachHost(host) {
        if (!racksByName.hasOwnProperty(host.rack)) {
            racksByName[host.rack] = {
                name: host.rack,
                hosts: [host],
                load: 0,
                owns: '0.00%'
            };
        } else {
            racksByName[host.rack].hosts.push(host);
        }
    });

    var racks = [];
    Object.keys(racksByName).forEach(function eachRackByName(rackName) {
        racks.push(racksByName[rackName]);
    });
    // Order racks by hosts count descending
    racks = racks.sort(function byCount(a, b) {
        return b.hosts.length - a.hosts.length;
    });

    var rackGroups = [];
    for (var i = 0; i < replicas; i++) {
        var rackGroup = {
            racks: []
        };
        rackGroups.push(rackGroup);
    }

    // Assign largest racks to rack group most required
    var unassignedRacks = racks.concat([]);
    while (unassignedRacks.length > 0) {
        var rackGroupsByHostCountAscending = rackGroups.sort(function byCount(a, b) {
            return rackGroupHosts(a).length - rackGroupHosts(b).length;
        });
        rackGroupsByHostCountAscending[0].racks.push(unassignedRacks.shift());
    }

    // Apply load
    for (var i = 0; i < replicas; i++) {
        var rackGroup = rackGroups[i];
        var targetHosts = rackGroupHosts(rackGroup);
        for (var j = 0; j < shards; j++) {
            var host = targetHosts[j % targetHosts.length];
            host.load = host.load+1;
            host.owns = ((host.load / (replicas * shards))*100).toFixed(2) + '%';
            var rack = racksByName[host.rack];
            rack.load = rack.load+1;
            rack.owns = ((rack.load / (replicas * shards))*100).toFixed(2) + '%';
        }
    }

    // Assign load
    for (var i = 0; i < replicas; i++) {
        var rackGroup = rackGroups[i];
        var targetHosts = rackGroupHosts(rackGroup);
        var unassignedReplicaSet = unassignedReplicaSets[i];
        targetHosts.forEach(function eachTargetHost(host) {
            for (var i = 0; i < host.load; i++) {
                var shard = unassignedReplicaSet.shards.shift();
                shard.host = {name: host.name, rack: host.rack};
                host.shards.push(shard);
            }
        });
    }

    return {
        racksSummary: summarizeRacksByName(racksByName),
        replicaSetsSummary: summarizeReplicaSets(replicaSets, racksByName, shards),
        racks: racks,
        hosts: hosts,
        replicaSets: replicaSets
    };

    function rackGroupHosts(rackGroup) {
        var result = [];
        rackGroup.racks.forEach(function eachRack(rack) {
            result = result.concat(rack.hosts);
        });
        return result;
    }
}

function createHosts(racks) {
    var id = 0;
    var hosts = [];
    Object.keys(racks).forEach(function eachRack(rack) {
        var count = racks[rack];
        var padLeft = createPadLeft(4);
        for (var i = 0; i < count; i++) {
            hosts.push({
                name: 'h' + padLeft(id++),
                rack: rack,
                shards: [],
                load: 0,
                owns: '0.00%'
            });
        }
    });
    return hosts;
}

function createPadLeft(n) {
    var prefix = '';
    for (var i = 0; i < n; i++) {
        prefix += '0';
    }
    return function padLeft(str) {
        var result = prefix + str;
        return result.slice(-1 * n);
    };
}

function inspect(data) {
    return util.inspect(data, {depth: 50, colors: true});
}

function summarizeRacksByName(racks) {
    var result = {};
    Object.keys(racks).forEach(function eachRackName(rackName) {
        var rack = racks[rackName];
        var entry = result[rackName] = {};
        Object.keys(rack).forEach(function eachRackKey(key) {
            if (key === 'hosts') {
                return;
            }
            entry[key] = rack[key];
        });
    });
    return result;
}

function summarizeReplicaSets(replicaSets, racksByName, shards) {
    return replicaSets.map(function eachReplicaSet(replicaSet, replica) {
        var result = {
            replica: replica,
            racks: [],
            hosts: 0
        };

        var replicaSetRacksByName = {};
        var replicaSetHostsByName = {};
        replicaSet.shards.forEach(function eachShard(shard) {
            var host = shard.host.name;
            var rack = shard.host.rack;

            if (!replicaSetRacksByName.hasOwnProperty(rack)) {
                replicaSetRacksByName[rack] = {
                    name: rack,
                    load: 1,
                    owns: ((1 / shards)*100).toFixed(2) + '%'
                };
            } else {
                replicaSetRacksByName[rack].load = replicaSetRacksByName[rack].load + 1;
                replicaSetRacksByName[rack].owns = ((replicaSetRacksByName[rack].load / shards)*100).toFixed(2) + '%';
            }

            if (!replicaSetHostsByName.hasOwnProperty(host)) {
                replicaSetHostsByName[host] = host;
                result.hosts = result.hosts + 1;
            }
        });

        Object.keys(replicaSetRacksByName).forEach(function eachRackName(rackName) {
            result.racks.push(replicaSetRacksByName[rackName]);
        });

        return result;
    });
}
