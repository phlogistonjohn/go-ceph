package builder

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var sample1 = `{
  "cmd000": {
    "sig": [
      "pg",
      "stat"
    ],
    "help": "show placement group status.",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd001": {
    "sig": [
      "pg",
      "getmap"
    ],
    "help": "get binary pg map to -o/stdout",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd002": {
    "sig": [
      "pg",
      "dump",
      {
        "n": "N",
        "name": "dumpcontents",
        "req": false,
        "strings": "all|summary|sum|delta|pools|osds|pgs|pgs_brief",
        "type": "CephChoices"
      }
    ],
    "help": "show human-readable versions of pg map (only 'all' valid with plain)",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd003": {
    "sig": [
      "pg",
      "dump_json",
      {
        "n": "N",
        "name": "dumpcontents",
        "req": false,
        "strings": "all|summary|sum|pools|osds|pgs",
        "type": "CephChoices"
      }
    ],
    "help": "show human-readable version of pg map in json only",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd004": {
    "sig": [
      "pg",
      "dump_pools_json"
    ],
    "help": "show pg pools info in json only",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd005": {
    "sig": [
      "pg",
      "ls-by-pool",
      {
        "name": "poolstr",
        "type": "CephString"
      },
      {
        "n": "N",
        "name": "states",
        "req": false,
        "type": "CephString"
      }
    ],
    "help": "list pg with pool = [poolname]",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd006": {
    "sig": [
      "pg",
      "ls-by-primary",
      {
        "name": "osd",
        "type": "CephOsdName"
      },
      {
        "name": "pool",
        "req": false,
        "type": "CephInt"
      },
      {
        "n": "N",
        "name": "states",
        "req": false,
        "type": "CephString"
      }
    ],
    "help": "list pg with primary = [osd]",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd007": {
    "sig": [
      "pg",
      "ls-by-osd",
      {
        "name": "osd",
        "type": "CephOsdName"
      },
      {
        "name": "pool",
        "req": false,
        "type": "CephInt"
      },
      {
        "n": "N",
        "name": "states",
        "req": false,
        "type": "CephString"
      }
    ],
    "help": "list pg on osd [osd]",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd008": {
    "sig": [
      "pg",
      "ls",
      {
        "name": "pool",
        "req": false,
        "type": "CephInt"
      },
      {
        "n": "N",
        "name": "states",
        "req": false,
        "type": "CephString"
      }
    ],
    "help": "list pg with specific pool, osd, state",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd009": {
    "sig": [
      "pg",
      "dump_stuck",
      {
        "n": "N",
        "name": "stuckops",
        "req": false,
        "strings": "inactive|unclean|stale|undersized|degraded",
        "type": "CephChoices"
      },
      {
        "name": "threshold",
        "req": false,
        "type": "CephInt"
      }
    ],
    "help": "show information about stuck pgs",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd010": {
    "sig": [
      "pg",
      "debug",
      {
        "name": "debugop",
        "strings": "unfound_objects_exist|degraded_pgs_exist",
        "type": "CephChoices"
      }
    ],
    "help": "show debug info about pgs",
    "module": "pg",
    "perm": "r",
    "flags": 8
  },
  "cmd011": {
    "sig": [
      "pg",
      "scrub",
      {
        "name": "pgid",
        "type": "CephPgid"
      }
    ],
    "help": "start scrub on <pgid>",
    "module": "pg",
    "perm": "rw",
    "flags": 8
  },
  "cmd012": {
    "sig": [
      "pg",
      "deep-scrub",
      {
        "name": "pgid",
        "type": "CephPgid"
      }
    ],
    "help": "start deep-scrub on <pgid>",
    "module": "pg",
    "perm": "rw",
    "flags": 8
  },
  "cmd013": {
    "sig": [
      "pg",
      "repair",
      {
        "name": "pgid",
        "type": "CephPgid"
      }
    ],
    "help": "start repair on <pgid>",
    "module": "pg",
    "perm": "rw",
    "flags": 8
  },
  "cmd014": {
    "sig": [
      "pg",
      "force-recovery",
      {
        "n": "N",
        "name": "pgid",
        "type": "CephPgid"
      }
    ],
    "help": "force recovery of <pgid> first",
    "module": "pg",
    "perm": "rw",
    "flags": 8
  },
  "cmd015": {
    "sig": [
      "pg",
      "force-backfill",
      {
        "n": "N",
        "name": "pgid",
        "type": "CephPgid"
      }
    ],
    "help": "force backfill of <pgid> first",
    "module": "pg",
    "perm": "rw",
    "flags": 8
  },
  "cmd016": {
    "sig": [
      "pg",
      "cancel-force-recovery",
      {
        "n": "N",
        "name": "pgid",
        "type": "CephPgid"
      }
    ],
    "help": "restore normal recovery priority of <pgid>",
    "module": "pg",
    "perm": "rw",
    "flags": 8
  },
  "cmd017": {
    "sig": [
      "pg",
      "cancel-force-backfill",
      {
        "n": "N",
        "name": "pgid",
        "type": "CephPgid"
      }
    ],
    "help": "restore normal backfill priority of <pgid>",
    "module": "pg",
    "perm": "rw",
    "flags": 8
  },
  "cmd018": {
    "sig": [
      "osd",
      "perf"
    ],
    "help": "print dump of OSD perf summary stats",
    "module": "osd",
    "perm": "r",
    "flags": 8
  },
  "cmd019": {
    "sig": [
      "osd",
      "df",
      {
        "name": "output_method",
        "req": false,
        "strings": "plain|tree",
        "type": "CephChoices"
      },
      {
        "name": "filter_by",
        "req": false,
        "strings": "class|name",
        "type": "CephChoices"
      },
      {
        "name": "filter",
        "req": false,
        "type": "CephString"
      }
    ],
    "help": "show OSD utilization",
    "module": "osd",
    "perm": "r",
    "flags": 8
  },
  "cmd020": {
    "sig": [
      "osd",
      "blocked-by"
    ],
    "help": "print histogram of which OSDs are blocking their peers",
    "module": "osd",
    "perm": "r",
    "flags": 8
  },
  "cmd021": {
    "sig": [
      "osd",
      "pool",
      "stats",
      {
        "name": "pool_name",
        "req": false,
        "type": "CephPoolname"
      }
    ],
    "help": "obtain stats from all pools, or from specified pool",
    "module": "osd",
    "perm": "r",
    "flags": 8
  },
  "cmd022": {
    "sig": [
      "osd",
      "pool",
      "scrub",
      {
        "n": "N",
        "name": "who",
        "type": "CephPoolname"
      }
    ],
    "help": "initiate scrub on pool <who>",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd023": {
    "sig": [
      "osd",
      "pool",
      "deep-scrub",
      {
        "n": "N",
        "name": "who",
        "type": "CephPoolname"
      }
    ],
    "help": "initiate deep-scrub on pool <who>",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd024": {
    "sig": [
      "osd",
      "pool",
      "repair",
      {
        "n": "N",
        "name": "who",
        "type": "CephPoolname"
      }
    ],
    "help": "initiate repair on pool <who>",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd025": {
    "sig": [
      "osd",
      "pool",
      "force-recovery",
      {
        "n": "N",
        "name": "who",
        "type": "CephPoolname"
      }
    ],
    "help": "force recovery of specified pool <who> first",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd026": {
    "sig": [
      "osd",
      "pool",
      "force-backfill",
      {
        "n": "N",
        "name": "who",
        "type": "CephPoolname"
      }
    ],
    "help": "force backfill of specified pool <who> first",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd027": {
    "sig": [
      "osd",
      "pool",
      "cancel-force-recovery",
      {
        "n": "N",
        "name": "who",
        "type": "CephPoolname"
      }
    ],
    "help": "restore normal recovery priority of specified pool <who>",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd028": {
    "sig": [
      "osd",
      "pool",
      "cancel-force-backfill",
      {
        "n": "N",
        "name": "who",
        "type": "CephPoolname"
      }
    ],
    "help": "restore normal recovery priority of specified pool <who>",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd029": {
    "sig": [
      "osd",
      "reweight-by-utilization",
      {
        "name": "oload",
        "req": false,
        "type": "CephInt"
      },
      {
        "name": "max_change",
        "req": false,
        "type": "CephFloat"
      },
      {
        "name": "max_osds",
        "req": false,
        "type": "CephInt"
      },
      {
        "name": "no_increasing",
        "req": false,
        "type": "CephBool"
      }
    ],
    "help": "reweight OSDs by utilization [overload-percentage-for-consideration, default 120]",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd030": {
    "sig": [
      "osd",
      "test-reweight-by-utilization",
      {
        "name": "oload",
        "req": false,
        "type": "CephInt"
      },
      {
        "name": "max_change",
        "req": false,
        "type": "CephFloat"
      },
      {
        "name": "max_osds",
        "req": false,
        "type": "CephInt"
      },
      {
        "name": "no_increasing",
        "req": false,
        "type": "CephBool"
      }
    ],
    "help": "dry run of reweight OSDs by utilization [overload-percentage-for-consideration, default 120]",
    "module": "osd",
    "perm": "r",
    "flags": 8
  },
  "cmd031": {
    "sig": [
      "osd",
      "reweight-by-pg",
      {
        "name": "oload",
        "req": false,
        "type": "CephInt"
      },
      {
        "name": "max_change",
        "req": false,
        "type": "CephFloat"
      },
      {
        "name": "max_osds",
        "req": false,
        "type": "CephInt"
      },
      {
        "n": "N",
        "name": "pools",
        "req": false,
        "type": "CephPoolname"
      }
    ],
    "help": "reweight OSDs by PG distribution [overload-percentage-for-consideration, default 120]",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd032": {
    "sig": [
      "osd",
      "test-reweight-by-pg",
      {
        "name": "oload",
        "req": false,
        "type": "CephInt"
      },
      {
        "name": "max_change",
        "req": false,
        "type": "CephFloat"
      },
      {
        "name": "max_osds",
        "req": false,
        "type": "CephInt"
      },
      {
        "n": "N",
        "name": "pools",
        "req": false,
        "type": "CephPoolname"
      }
    ],
    "help": "dry run of reweight OSDs by PG distribution [overload-percentage-for-consideration, default 120]",
    "module": "osd",
    "perm": "r",
    "flags": 8
  },
  "cmd033": {
    "sig": [
      "osd",
      "destroy",
      {
        "name": "id",
        "type": "CephOsdName"
      },
      {
        "name": "force",
        "req": false,
        "type": "CephBool"
      },
      {
        "name": "yes_i_really_mean_it",
        "req": false,
        "type": "CephBool"
      }
    ],
    "help": "mark osd as being destroyed. Keeps the ID intact (allowing reuse), but removes cephx keys, config-key data and lockbox keys, rendering data permanently unreadable.",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd034": {
    "sig": [
      "osd",
      "purge",
      {
        "name": "id",
        "type": "CephOsdName"
      },
      {
        "name": "force",
        "req": false,
        "type": "CephBool"
      },
      {
        "name": "yes_i_really_mean_it",
        "req": false,
        "type": "CephBool"
      }
    ],
    "help": "purge all osd data from the monitors including the OSD id and CRUSH position",
    "module": "osd",
    "perm": "rw",
    "flags": 8
  },
  "cmd035": {
    "sig": [
      "osd",
      "safe-to-destroy",
      {
        "n": "N",
        "name": "ids",
        "type": "CephString"
      }
    ],
    "help": "check whether osd(s) can be safely destroyed without reducing data durability",
    "module": "osd",
    "perm": "r",
    "flags": 8
  },
  "cmd036": {
    "sig": [
      "osd",
      "ok-to-stop",
      {
        "n": "N",
        "name": "ids",
        "type": "CephString"
      },
      {
        "name": "max",
        "req": false,
        "type": "CephInt"
      }
    ],
    "help": "check whether osd(s) can be safely stopped without reducing immediate data availability",
    "module": "osd",
    "perm": "r",
    "flags": 8
  }
}`

func TestDescriptionUnmarshal(t *testing.T) {
	cde := CommandDescriptions{}
	b := []byte(sample1)
	assert.NoError(t, json.Unmarshal(b, &cde))
	assert.Len(t, cde.Entries, 37)

	t.Run("cmd000", func(t *testing.T) {
		d1 := cde.Entries[0]
		assert.EqualValues(t, d1.Key, "cmd000")
		assert.EqualValues(t, d1.Module, "pg")
		assert.EqualValues(t, d1.Perm, "r")
		assert.EqualValues(t, d1.Flags, 8)
		assert.EqualValues(t, d1.Prefix(), []string{"pg", "stat"})
		assert.Empty(t, d1.Variables())
	})

	t.Run("cmd003", func(t *testing.T) {
		d1 := cde.Entries[3]
		assert.EqualValues(t, d1.Key, "cmd003")
		assert.EqualValues(t, d1.Module, "pg")
		assert.EqualValues(t, d1.Perm, "r")
		assert.EqualValues(t, d1.Flags, 8)
		assert.EqualValues(t, d1.Prefix(), []string{"pg", "dump_json"})
		assert.EqualValues(t, d1.PrefixString(), "pg dump_json")
		vars := d1.Variables()
		assert.Len(t, vars, 1)
		assert.EqualValues(t, vars[0].Name, "dumpcontents")
		assert.EqualValues(t, vars[0].Type, "CephChoices")
		assert.EqualValues(t, vars[0].Choices, "all|summary|sum|pools|osds|pgs")
		assert.EqualValues(t, vars[0].Required(), false)
		assert.EqualValues(t, vars[0].Repeat, "N")
	})
}

func TestDescriptionFind(t *testing.T) {
	cde := CommandDescriptions{}
	b := []byte(sample1)
	assert.NoError(t, json.Unmarshal(b, &cde))
	assert.Len(t, cde.Entries, 37)

	matches := cde.Find("osd")
	assert.Len(t, matches, 19)

	matches = cde.Find("osd", "pool")
	assert.Len(t, matches, 8)

	matches = cde.Find("osd", "pool", "scrub")
	assert.Len(t, matches, 1)
}

/* LAZY ZONE ---------------------------------- */

func TestBuilder1(t *testing.T) {
	cde := CommandDescriptions{}
	b := []byte(sample1)
	assert.NoError(t, json.Unmarshal(b, &cde))
	assert.Len(t, cde.Entries, 37)

	matches := cde.Find("osd", "reweight-by-utilization")
	assert.Len(t, matches, 1)

	bld := NewBuilder(matches[0])
	argTypes := bld.Arguments()
	assert.Len(t, argTypes, 4)

	assert.EqualValues(t, "CephInt", argTypes[0].TypeName())
	assert.NoError(t, argTypes[0].Set(bld.Values, 44))
	assert.NoError(t, argTypes[0].Set(bld.Values, "11"))
	assert.Error(t, argTypes[0].Set(bld.Values, nil))
	assert.NoError(t, argTypes[0].Validate(bld.Values))

	assert.EqualValues(t, "CephFloat", argTypes[1].TypeName())
	assert.NoError(t, argTypes[1].Set(bld.Values, 4.4))
	assert.NoError(t, argTypes[1].Set(bld.Values, "11"))
	assert.Error(t, argTypes[1].Set(bld.Values, nil))
	assert.NoError(t, argTypes[1].Validate(bld.Values))

	assert.EqualValues(t, "CephInt", argTypes[2].TypeName())

	assert.EqualValues(t, "CephBool", argTypes[3].TypeName())
	assert.NoError(t, argTypes[3].Set(bld.Values, true))
	assert.NoError(t, argTypes[3].Set(bld.Values, "false"))
	assert.Error(t, argTypes[3].Set(bld.Values, nil))
	assert.NoError(t, argTypes[3].Validate(bld.Values))

	bld.Values["no_increasing"] = nil
	assert.Error(t, argTypes[3].Validate(bld.Values))
}

func TestBuilder2(t *testing.T) {
	cde := CommandDescriptions{}
	b := []byte(sample1)
	assert.NoError(t, json.Unmarshal(b, &cde))
	assert.Len(t, cde.Entries, 37)

	matches := cde.Find("osd", "pool", "scrub")
	assert.Len(t, matches, 1)

	bld := NewBuilder(matches[0])
	argTypes := bld.Arguments()
	assert.Len(t, argTypes, 1)

	cat := argTypes[0]
	assert.EqualValues(t, "CephPoolname (Repeat: N)", cat.TypeName())
	assert.NoError(t, cat.Set(bld.Values, []string{"foo", "bar"}))
	assert.NoError(t, cat.Set(bld.Values, "bloop"))
	assert.EqualValues(t, []any{"foo", "bar", "bloop"}, bld.Values[cat.Name()])
}

func TestBuilder3(t *testing.T) {
	cde := CommandDescriptions{}
	b := []byte(sample1)
	assert.NoError(t, json.Unmarshal(b, &cde))
	assert.Len(t, cde.Entries, 37)

	matches := cde.Find("pg", "dump_json")
	assert.Len(t, matches, 1)

	bld := NewBuilder(matches[0])
	argTypes := bld.Arguments()
	assert.Len(t, argTypes, 1)

	cat := argTypes[0]
	assert.EqualValues(t, "CephChoices (Repeat: N)", cat.TypeName())
	assert.Error(t, cat.Set(bld.Values, "bronco"))
	assert.Error(t, cat.Set(bld.Values, "splatter"))
	assert.NoError(t, cat.Set(bld.Values, "all"))
	assert.NoError(t, cat.Set(bld.Values, "summary"))
	assert.EqualValues(t, []any{"all", "summary"}, bld.Values[cat.Name()])
	assert.NoError(t, cat.Validate(bld.Values))
}
