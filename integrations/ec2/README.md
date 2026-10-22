# EC2 integration

This Go module will hold the optional skygo provider for one owned EC2
virtual machine. It contains `internal/ec2api`, a private EC2 and STS Query client,
and `internal/ec2vm`, a lifecycle driver whose caller must persist its
records. Store integration is not implemented. There is no command, no
registered target and no provisioning: nothing in this module creates an instance on its own, and it
has not been run against AWS.

`ec2api` builds an immutable RunInstances request from a fixed target
profile and an environment identity. The request carries a ClientToken
derived from the identity and tags the instance, root volume and network
interface at creation. The client signs requests with the AWS SDK for Go v2
SigV4 signer, makes exactly one HTTP attempt per call and never retries.
Before every create, stop, start and terminate it checks that the
credentials belong to the profile's account; before a stop, start,
terminate or console read it also describes the exact instance ID and
refuses unless the account, region, ClientToken, every tag, image, type,
subnet, key pair and security groups match.

The client reads no AWS configuration files or environment variables, and
it uses no instance metadata service or proxy. The caller supplies the
credentials.

Test from the repository root:

```sh
(cd integrations/ec2 && GOWORK=off go test ./...)
```

Tests use local TLS endpoints that verify every request signature and
fixture credentials only. They model the EC2 behavior the client depends on;
they are not evidence of AWS behavior.
