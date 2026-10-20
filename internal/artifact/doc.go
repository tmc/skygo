// Package artifact validates bounded uncompressed USTAR snapshots and copies
// them through opened directory roots. Extract and Collect require Linux or
// Darwin. Their callers own operation limits, durable authority and publication.
//
// A Root confines path resolution; it is not a workload sandbox. Collection
// requires independently established writer quiescence. Metadata checks reject
// observed replacement, not every transient content change or file provenance.
//
// Files can retain one bounded PhysicalScan of its original root. The result is
// an interval observation of allocated inode blocks, not an atomic current sum,
// hard quota, execution ownership or cleanup proof. A yielded cursor still owns
// original descriptors until positive Close. Publication uses a caller-supplied
// cached growth check only immediately before creating a new object link.
package artifact
