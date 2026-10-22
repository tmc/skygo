// Package workeradmission decides all-or-nothing admission of worker groups
// against an operator-declared per-target budget.
//
// A group is admitted whole or not at all. Admit walks the queue in order and
// stops at the first group whose whole demand does not fit what remains after
// every held reservation; a blocked head blocks every later group. A zero
// budget field is zero capacity, not unlimited. Held reservations stay charged,
// including those whose members are held or Unknown, until the caller records
// a positive stop or never-launched proof for every member.
//
// The budget is operator configuration, not live cluster capacity, and an
// admission is not a capacity reservation in the cluster. Admission does not
// schedule or co-bind members, and it does not delay any member's main
// program until the others are bound; those are separate later steps.
//
// The package is pure arithmetic: no clock, storage or I/O.
package workeradmission
