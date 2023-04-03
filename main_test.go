package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMain_convertPgLsn(t *testing.T) {
	// pg_lsn supports values between 0/0 and FFFFFFFF/FFFFFFFF.

	// SELECT '0/0'::pg_lsn - '0/0'; 0
	assert.Equal(t, uint64(0), parsePgLsn(""))
	assert.Equal(t, uint64(0), parsePgLsn("0/0"))

	// DEBUG> checking: master.currentWalLsn=`0/189B2E78`, slave.lastWalReceiveLsn=`0/90000A0`, slave.lastWalReplayLsn=`0/90000A0`
	// DEBUG> checking: slave.behind: `-261828056`, slave.delay: `0`

	// master.currentWalLsn=`0/189B2E78`
	// SELECT '0/189B2E78'::pg_lsn - '0/0'; -- 412823160
	assert.Equal(t, uint64(412_823_160), parsePgLsn("0/189B2E78"))

	// slave.lastWalReceiveLsn=`0/90000A0`, slave.lastWalReplayLsn=`0/90000A0`
	// SELECT '0/90000A0'::pg_lsn - '0/0'; -- 150995104
	assert.Equal(t, uint64(150_995_104), parsePgLsn("0/90000A0"))

	// SELECT '0/FFFFFFFF'::pg_lsn - '0/0'; -- 4294967295
	assert.Equal(t, uint64(4_294_967_295), parsePgLsn("0/FFFFFFFF"))

	// SELECT '7/A25801C8'::pg_lsn - '0/0'; -- 32788447688
	assert.Equal(t, uint64(32_788_447_688), parsePgLsn("7/A25801C8"))

	//  SELECT 'FFFFFFFF/0'::pg_lsn - '0/0'; -- 18446744069414584320
	assert.Equal(t, uint64(18_446_744_069_414_584_320), parsePgLsn("FFFFFFFF/0"))

	//  SELECT 'FFFFFFFF/FFFFFFFF'::pg_lsn - '0/0'; -- 18446744073709551615
	assert.Equal(t, uint64(18_446_744_073_709_551_615), parsePgLsn("FFFFFFFF/FFFFFFFF"))
}

func TestMain_calculateLag(t *testing.T) {
	m := Master{
		name:             "testMaster",
		currentWalLsnInt: parsePgLsn("0/189B2E78"), // 412_823_160
	}
	s := Slave{
		name:                 "testSlave",
		lastWalReceiveLsnInt: parsePgLsn("0/90000A1"), // 150_995_105
		lastWalReplayLsnInt:  parsePgLsn("0/90000A0"), // 150_995_104
	}
	receiveLag, replayLag := s.CalculateLag(m)

	assert.Equal(t, uint64(261_828_055), receiveLag)
	assert.Equal(t, uint64(1), replayLag)
}
