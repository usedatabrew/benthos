package aws

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid"

	"github.com/usedatabrew/benthos/v4/internal/component"
	"github.com/usedatabrew/benthos/v4/internal/impl/aws/config"
	"github.com/usedatabrew/benthos/v4/internal/old/util/retries"
	"github.com/usedatabrew/benthos/v4/public/service"
)

const (
	// Kinesis Input DynDB Fields
	kiddbFieldTable              = "table"
	kiddbFieldCreate             = "create"
	kiddbFieldReadCapacityUnits  = "read_capacity_units"
	kiddbFieldWriteCapacityUnits = "write_capacity_units"
	kiddbFieldBillingMode        = "billing_mode"

	// Kinesis Input Fields
	kiFieldDynamoDB        = "dynamodb"
	kiFieldStreams         = "streams"
	kiFieldCheckpointLimit = "checkpoint_limit"
	kiFieldCommitPeriod    = "commit_period"
	kiFieldLeasePeriod     = "lease_period"
	kiFieldRebalancePeriod = "rebalance_period"
	kiFieldStartFromOldest = "start_from_oldest"
	kiFieldBatching        = "batching"
)

type kiConfig struct {
	Streams         []string
	DynamoDB        kiddbConfig
	CheckpointLimit int
	CommitPeriod    string
	LeasePeriod     string
	RebalancePeriod string
	StartFromOldest bool
}

func kinesisInputConfigFromParsed(pConf *service.ParsedConfig) (conf kiConfig, err error) {
	if conf.Streams, err = pConf.FieldStringList(kiFieldStreams); err != nil {
		return
	}
	if pConf.Contains(kiFieldDynamoDB) {
		if conf.DynamoDB, err = kinesisInputDynamoDBConfigFromParsed(pConf.Namespace(kiFieldDynamoDB)); err != nil {
			return
		}
	}
	if conf.CheckpointLimit, err = pConf.FieldInt(kiFieldCheckpointLimit); err != nil {
		return
	}
	if conf.CommitPeriod, err = pConf.FieldString(kiFieldCommitPeriod); err != nil {
		return
	}
	if conf.LeasePeriod, err = pConf.FieldString(kiFieldLeasePeriod); err != nil {
		return
	}
	if conf.RebalancePeriod, err = pConf.FieldString(kiFieldRebalancePeriod); err != nil {
		return
	}
	if conf.StartFromOldest, err = pConf.FieldBool(kiFieldStartFromOldest); err != nil {
		return
	}
	return
}

func kinesisInputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary("Receive messages from one or more Kinesis streams.").
		Description(`
Consumes messages from one or more Kinesis streams either by automatically balancing shards across other instances of this input, or by consuming shards listed explicitly. The latest message sequence consumed by this input is stored within a [DynamoDB table](#table-schema), which allows it to resume at the correct sequence of the shard during restarts. This table is also used for coordination across distributed inputs when shard balancing.

Benthos will not store a consumed sequence unless it is acknowledged at the output level, which ensures at-least-once delivery guarantees.

### Ordering

By default messages of a shard can be processed in parallel, up to a limit determined by the field `+"`checkpoint_limit`"+`. However, if strict ordered processing is required then this value must be set to 1 in order to process shard messages in lock-step. When doing so it is recommended that you perform batching at this component for performance as it will not be possible to batch lock-stepped messages at the output level.

### Table Schema

It's possible to configure Benthos to create the DynamoDB table required for coordination if it does not already exist. However, if you wish to create this yourself (recommended) then create a table with a string HASH key `+"`StreamID`"+` and a string RANGE key `+"`ShardID`"+`.

### Batching

Use the `+"`batching`"+` fields to configure an optional [batching policy](/docs/configuration/batching#batch-policy). Each stream shard will be batched separately in order to ensure that acknowledgements aren't contaminated.
`).Fields(
		service.NewStringListField(kiFieldStreams).
			Description("One or more Kinesis data streams to consume from. Shards of a stream are automatically balanced across consumers by coordinating through the provided DynamoDB table. Multiple comma separated streams can be listed in a single element. Shards are automatically distributed across consumers of a stream by coordinating through the provided DynamoDB table. Alternatively, it's possible to specify an explicit shard to consume from with a colon after the stream name, e.g. `foo:0` would consume the shard `0` of the stream `foo`."),
		service.NewObjectField(kiFieldDynamoDB,
			service.NewStringField(kiddbFieldTable).
				Description("The name of the table to access.").
				Default(""),
			service.NewBoolField(kiddbFieldCreate).
				Description("Whether, if the table does not exist, it should be created.").
				Default(false),
			service.NewStringEnumField(kiddbFieldBillingMode, "PROVISIONED", "PAY_PER_REQUEST").
				Description("When creating the table determines the billing mode.").
				Default("PAY_PER_REQUEST").
				Advanced(),
			service.NewIntField(kiddbFieldReadCapacityUnits).
				Description("Set the provisioned read capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
				Default(0).
				Advanced(),
			service.NewIntField(kiddbFieldWriteCapacityUnits).
				Description("Set the provisioned write capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
				Default(0).
				Advanced(),
		).
			Description("Determines the table used for storing and accessing the latest consumed sequence for shards, and for coordinating balanced consumers of streams."),
		service.NewIntField(kiFieldCheckpointLimit).
			Description("The maximum gap between the in flight sequence versus the latest acknowledged sequence at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual shards. Any given sequence will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024),
		service.NewDurationField(kiFieldCommitPeriod).
			Description("The period of time between each update to the checkpoint table.").
			Default("5s"),
		service.NewDurationField(kiFieldRebalancePeriod).
			Description("The period of time between each attempt to rebalance shards across clients.").
			Default("30s").
			Advanced(),
		service.NewDurationField(kiFieldLeasePeriod).
			Description("The period of time after which a client that has failed to update a shard checkpoint is assumed to be inactive.").
			Default("30s").
			Advanced(),
		service.NewBoolField(kiFieldStartFromOldest).
			Description("Whether to consume from the oldest message when a sequence does not yet exist for the stream.").
			Default(true),
	).
		Fields(config.SessionFields()...).
		Field(service.NewBatchPolicyField(kiFieldBatching))
	return spec
}

func init() {
	err := service.RegisterBatchInput("aws_kinesis", kinesisInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newKinesisReaderFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(r), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

var awsKinesisDefaultLimit = int64(10e3)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type kinesisReader struct {
	conf     kiConfig
	clientID string

	sess    *session.Session
	batcher service.BatchPolicy
	log     *service.Logger
	mgr     *service.Resources

	backoffCtor func() backoff.BackOff
	boffPool    sync.Pool

	svc          kinesisiface.KinesisAPI
	checkpointer *awsKinesisCheckpointer

	streamShards    map[string][]string
	balancedStreams []string

	commitPeriod    time.Duration
	leasePeriod     time.Duration
	rebalancePeriod time.Duration

	cMut    sync.Mutex
	msgChan chan asyncMessage

	ctx  context.Context
	done func()

	closeOnce  sync.Once
	closedChan chan struct{}
}

var errCannotMixBalancedShards = errors.New("it is not currently possible to include balanced and explicit shard streams in the same kinesis input")

func newKinesisReaderFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*kinesisReader, error) {
	conf, err := kinesisInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	sess, err := GetSession(pConf)
	if err != nil {
		return nil, err
	}
	batcher, err := pConf.FieldBatchPolicy(kiFieldBatching)
	if err != nil {
		return nil, err
	}
	return newKinesisReaderFromConfig(conf, batcher, sess, mgr)
}

func newKinesisReaderFromConfig(conf kiConfig, batcher service.BatchPolicy, sess *session.Session, mgr *service.Resources) (*kinesisReader, error) {
	if batcher.IsNoop() {
		batcher.Count = 1
	}

	k := kinesisReader{
		conf:         conf,
		sess:         sess,
		batcher:      batcher,
		log:          mgr.Logger(),
		mgr:          mgr,
		closedChan:   make(chan struct{}),
		streamShards: map[string][]string{},
	}
	k.ctx, k.done = context.WithCancel(context.Background())

	u4, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	k.clientID = u4.String()

	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "300ms"
	rConf.Backoff.MaxInterval = "5s"
	if k.backoffCtor, err = rConf.GetCtor(); err != nil {
		return nil, err
	}
	k.boffPool = sync.Pool{
		New: func() any {
			return k.backoffCtor()
		},
	}

	for _, t := range conf.Streams {
		for _, splitStreams := range strings.Split(t, ",") {
			if trimmed := strings.TrimSpace(splitStreams); len(trimmed) > 0 {
				if withShards := strings.Split(trimmed, ":"); len(withShards) > 1 {
					if len(k.balancedStreams) > 0 {
						return nil, errCannotMixBalancedShards
					}
					if len(withShards) > 2 {
						return nil, fmt.Errorf("stream '%v' is invalid, only one shard should be specified and the same stream can be listed multiple times, e.g. use `foo:0,foo:1` not `foo:0:1`", trimmed)
					}
					stream := strings.TrimSpace(withShards[0])
					shard := strings.TrimSpace(withShards[1])
					k.streamShards[stream] = append(k.streamShards[stream], shard)
				} else {
					if len(k.streamShards) > 0 {
						return nil, errCannotMixBalancedShards
					}
					k.balancedStreams = append(k.balancedStreams, trimmed)
				}
			}
		}
	}
	if k.commitPeriod, err = time.ParseDuration(k.conf.CommitPeriod); err != nil {
		return nil, fmt.Errorf("failed to parse commit period string: %v", err)
	}
	if k.leasePeriod, err = time.ParseDuration(k.conf.LeasePeriod); err != nil {
		return nil, fmt.Errorf("failed to parse lease period string: %v", err)
	}
	if k.rebalancePeriod, err = time.ParseDuration(k.conf.RebalancePeriod); err != nil {
		return nil, fmt.Errorf("failed to parse rebalance period string: %v", err)
	}
	return &k, nil
}

//------------------------------------------------------------------------------

const (
	// ErrCodeKMSThrottlingException is defined in the API Reference
	// https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.GetRecords
	ErrCodeKMSThrottlingException = "KMSThrottlingException"
)

func (k *kinesisReader) getIter(streamID, shardID, sequence string) (string, error) {
	iterType := kinesis.ShardIteratorTypeTrimHorizon
	if !k.conf.StartFromOldest {
		iterType = kinesis.ShardIteratorTypeLatest
	}
	var startingSequence *string
	if len(sequence) > 0 {
		iterType = kinesis.ShardIteratorTypeAfterSequenceNumber
		startingSequence = &sequence
	}

	res, err := k.svc.GetShardIteratorWithContext(k.ctx, &kinesis.GetShardIteratorInput{
		StreamName:             &streamID,
		ShardId:                &shardID,
		StartingSequenceNumber: startingSequence,
		ShardIteratorType:      &iterType,
	})
	if err != nil {
		return "", err
	}

	var iter string
	if res.ShardIterator != nil {
		iter = *res.ShardIterator
	}
	if iter == "" {
		// If we failed to obtain from a sequence we start from beginning
		iterType = kinesis.ShardIteratorTypeTrimHorizon

		res, err := k.svc.GetShardIteratorWithContext(k.ctx, &kinesis.GetShardIteratorInput{
			StreamName:        &streamID,
			ShardId:           &shardID,
			ShardIteratorType: &iterType,
		})
		if err != nil {
			return "", err
		}

		if res.ShardIterator != nil {
			iter = *res.ShardIterator
		}
	}
	if iter == "" {
		return "", errors.New("failed to obtain shard iterator")
	}
	return iter, nil
}

// IMPORTANT TO NOTE: The returned shard iterator (second return parameter) will
// always be the input iterator when the error parameter is nil, therefore
// replacing the current iterator with this return param should always be safe.
//
// Do NOT modify this method without preserving this behaviour.
func (k *kinesisReader) getRecords(streamID, shardID, shardIter string) ([]*kinesis.Record, string, error) {
	res, err := k.svc.GetRecordsWithContext(k.ctx, &kinesis.GetRecordsInput{
		Limit:         &awsKinesisDefaultLimit,
		ShardIterator: &shardIter,
	})
	if err != nil {
		return nil, shardIter, err
	}

	nextIter := ""
	if res.NextShardIterator != nil {
		nextIter = *res.NextShardIterator
	}
	return res.Records, nextIter, nil
}

func awsErrIsTimeout(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, component.ErrTimeout) ||
		(err != nil && strings.HasSuffix(err.Error(), "context canceled"))
}

type awsKinesisConsumerState int

const (
	awsKinesisConsumerConsuming awsKinesisConsumerState = iota
	awsKinesisConsumerYielding
	awsKinesisConsumerFinished
	awsKinesisConsumerClosing
)

func (k *kinesisReader) runConsumer(wg *sync.WaitGroup, streamID, shardID, startingSequence string) (initErr error) {
	defer func() {
		if initErr != nil {
			wg.Done()
			if _, err := k.checkpointer.Checkpoint(context.Background(), streamID, shardID, startingSequence, true); err != nil {
				k.log.Errorf("Failed to gracefully yield checkpoint: %v\n", err)
			}
		}
	}()

	// Stores records, batches them up, and provides the batches for dispatch,
	// whilst ensuring only N records are in flight at a given time.
	var recordBatcher *awsKinesisRecordBatcher
	if recordBatcher, initErr = k.newAWSKinesisRecordBatcher(streamID, shardID, startingSequence); initErr != nil {
		return initErr
	}

	// Keeps track of retry attempts.
	boff := k.boffPool.Get().(backoff.BackOff)

	// Stores consumed records that have yet to be added to the batcher.
	var pending []*kinesis.Record
	var iter string
	if iter, initErr = k.getIter(streamID, shardID, startingSequence); initErr != nil {
		return initErr
	}

	// Keeps track of the latest state of the consumer.
	state := awsKinesisConsumerConsuming
	var pendingMsg asyncMessage

	unblockedChan, blockedChan := make(chan time.Time), make(chan time.Time)
	close(unblockedChan)

	// Channels (and contexts) representing the four main actions of the
	// consumer goroutine:
	// 1. Timed batches, this might be nil when timed batches are disabled.
	// 2. Record pulling, this might be unblocked (closed channel) when we run
	//    out of pending records, or a timed channel when our last attempt
	//    yielded zero records.
	// 3. Message flush, this is the target of our current batched message, and
	//    is nil when our current batched message is a zero value (we don't have
	//    one prepared).
	// 4. Next commit, is "done" when the next commit is due.
	var nextTimedBatchChan <-chan time.Time
	var nextPullChan <-chan time.Time = unblockedChan
	var nextFlushChan chan<- asyncMessage
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, k.commitPeriod)

	go func() {
		defer func() {
			commitCtxClose()
			recordBatcher.Close(context.Background(), state == awsKinesisConsumerFinished)
			boff.Reset()
			k.boffPool.Put(boff)

			reason := ""
			switch state {
			case awsKinesisConsumerFinished:
				reason = " because the shard is closed"
				if err := k.checkpointer.Delete(k.ctx, streamID, shardID); err != nil {
					k.log.Errorf("Failed to remove checkpoint for finished stream '%v' shard '%v': %v\n", streamID, shardID, err)
				}
			case awsKinesisConsumerYielding:
				reason = " because the shard has been claimed by another client"
				if err := k.checkpointer.Yield(k.ctx, streamID, shardID, recordBatcher.GetSequence()); err != nil {
					k.log.Errorf("Failed to yield checkpoint for stolen stream '%v' shard '%v': %v\n", streamID, shardID, err)
				}
			case awsKinesisConsumerClosing:
				reason = " because the pipeline is shutting down"
				if _, err := k.checkpointer.Checkpoint(context.Background(), streamID, shardID, recordBatcher.GetSequence(), true); err != nil {
					k.log.Errorf("Failed to store final checkpoint for stream '%v' shard '%v': %v\n", streamID, shardID, err)
				}
			}

			wg.Done()
			k.log.Debugf("Closing stream '%v' shard '%v' as client '%v'%v\n", streamID, shardID, k.checkpointer.clientID, reason)
		}()

		k.log.Debugf("Consuming stream '%v' shard '%v' as client '%v'\n", streamID, shardID, k.checkpointer.clientID)

		// Switches our pull chan to unblocked only if it's currently blocked,
		// as otherwise it's set to a timed channel that we do not want to
		// disturb.
		unblockPullChan := func() {
			if nextPullChan == blockedChan {
				nextPullChan = unblockedChan
			}
		}

		for {
			var err error
			if state == awsKinesisConsumerConsuming && len(pending) == 0 && nextPullChan == unblockedChan {
				if pending, iter, err = k.getRecords(streamID, shardID, iter); err != nil {
					if !awsErrIsTimeout(err) {
						nextPullChan = time.After(boff.NextBackOff())

						if aerr, ok := err.(awserr.Error); ok && aerr.Code() == kinesis.ErrCodeExpiredIteratorException {
							k.log.Warn("Shard iterator expired, attempting to refresh")
							newIter, err := k.getIter(streamID, shardID, recordBatcher.GetSequence())
							if err != nil {
								k.log.Errorf("Failed to refresh shard iterator: %v", err)
							} else {
								iter = newIter
							}
						} else {
							k.log.Errorf("Failed to pull Kinesis records: %v\n", err)
						}
					}
				} else if len(pending) == 0 {
					nextPullChan = time.After(boff.NextBackOff())
				} else {
					boff.Reset()
					nextPullChan = blockedChan
				}
				// The getRecords method ensures that it returns the input
				// iterator whenever it errors out. Therefore, regardless of the
				// outcome of the call if iter is now empty we have definitely
				// reached the end of the shard.
				if iter == "" {
					state = awsKinesisConsumerFinished
				}
			} else {
				unblockPullChan()
			}

			if pendingMsg.msg == nil {
				// If our consumer is finished and we've run out of pending
				// records then we're done.
				if len(pending) == 0 && state == awsKinesisConsumerFinished {
					if pendingMsg, _ = recordBatcher.FlushMessage(k.ctx); pendingMsg.msg == nil {
						return
					}
				} else if recordBatcher.HasPendingMessage() {
					if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
						k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
					}
				} else if len(pending) > 0 {
					var i int
					var r *kinesis.Record
					for i, r = range pending {
						if recordBatcher.AddRecord(r) {
							if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
								k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
							}
							break
						}
					}
					if pending = pending[i+1:]; len(pending) == 0 {
						unblockPullChan()
					}
				} else {
					unblockPullChan()
				}
			}

			if pendingMsg.msg != nil {
				nextFlushChan = k.msgChan
			} else {
				nextFlushChan = nil
			}

			if nextTimedBatchChan == nil {
				if tNext, exists := recordBatcher.UntilNext(); exists {
					nextTimedBatchChan = time.After(tNext)
				}
			}

			select {
			case <-commitCtx.Done():
				if k.ctx.Err() != nil {
					// It could've been our parent context that closed, in which
					// case we exit.
					state = awsKinesisConsumerClosing
					return
				}

				commitCtxClose()
				commitCtx, commitCtxClose = context.WithTimeout(k.ctx, k.commitPeriod)

				stillOwned, err := k.checkpointer.Checkpoint(k.ctx, streamID, shardID, recordBatcher.GetSequence(), false)
				if err != nil {
					k.log.Errorf("Failed to store checkpoint for Kinesis stream '%v' shard '%v': %v\n", streamID, shardID, err)
				} else if !stillOwned {
					state = awsKinesisConsumerYielding
					return
				}
			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil
			case nextFlushChan <- pendingMsg:
				pendingMsg = asyncMessage{}
			case <-nextPullChan:
				nextPullChan = unblockedChan
			case <-k.ctx.Done():
				state = awsKinesisConsumerClosing
				return
			}
		}
	}()
	return nil
}

//------------------------------------------------------------------------------

func isShardFinished(s *kinesis.Shard) bool {
	if s.SequenceNumberRange == nil {
		return false
	}
	if s.SequenceNumberRange.EndingSequenceNumber == nil {
		return false
	}
	return *s.SequenceNumberRange.EndingSequenceNumber != "null"
}

func (k *kinesisReader) runBalancedShards() {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		k.closeOnce.Do(func() {
			close(k.msgChan)
			close(k.closedChan)
		})
	}()

	for {
		for _, streamID := range k.balancedStreams {
			shardsRes, err := k.svc.ListShardsWithContext(k.ctx, &kinesis.ListShardsInput{
				StreamName: aws.String(streamID),
			})

			var clientClaims map[string][]awsKinesisClientClaim
			if err == nil {
				clientClaims, err = k.checkpointer.AllClaims(k.ctx, streamID)
			}
			if err != nil {
				if k.ctx.Err() != nil {
					return
				}
				k.log.Errorf("Failed to obtain stream '%v' shards or claims: %v\n", streamID, err)
				continue
			}

			totalShards := len(shardsRes.Shards)
			unclaimedShards := make(map[string]string, totalShards)
			for _, s := range shardsRes.Shards {
				if !isShardFinished(s) {
					unclaimedShards[*s.ShardId] = ""
				}
			}
			for clientID, claims := range clientClaims {
				for _, claim := range claims {
					if time.Since(claim.LeaseTimeout) > k.leasePeriod*2 {
						unclaimedShards[claim.ShardID] = clientID
					} else {
						delete(unclaimedShards, claim.ShardID)
					}
				}
			}

			// Have a go at grabbing any unclaimed shards
			if len(unclaimedShards) > 0 {
				for shardID, clientID := range unclaimedShards {
					sequence, err := k.checkpointer.Claim(k.ctx, streamID, shardID, clientID)
					if err != nil {
						if k.ctx.Err() != nil {
							return
						}
						if !errors.Is(err, ErrLeaseNotAcquired) {
							k.log.Errorf("Failed to claim unclaimed shard '%v': %v\n", shardID, err)
						}
						continue
					}
					wg.Add(1)
					if err = k.runConsumer(&wg, streamID, shardID, sequence); err != nil {
						k.log.Errorf("Failed to start consumer: %v\n", err)
					}
				}

				// If there are unclaimed shards then let's not resort to
				// thievery just yet.
				continue
			}

			// There were no unclaimed shards, let's look for a shard to steal.
			selfClaims := len(clientClaims[k.clientID])
			for clientID, claims := range clientClaims {
				if clientID == k.clientID {
					// Don't steal from ourself, we're not at that point yet.
					continue
				}

				// This is an extremely naive "algorithm", we simply randomly
				// iterate all other clients with shards and if any have two
				// more shards than we do then it's fair game. Using two here
				// so that we don't play hot potatoes with an odd shard.
				if len(claims) > (selfClaims + 1) {
					randomShard := claims[(rand.Int() % len(claims))].ShardID
					k.log.Debugf(
						"Attempting to steal stream '%v' shard '%v' from client '%v' as client '%v'\n",
						streamID, randomShard, clientID, k.clientID,
					)

					sequence, err := k.checkpointer.Claim(k.ctx, streamID, randomShard, clientID)
					if err != nil {
						if k.ctx.Err() != nil {
							return
						}
						if !errors.Is(err, ErrLeaseNotAcquired) {
							k.log.Errorf("Failed to steal shard '%v': %v\n", randomShard, err)
						}
						k.log.Debugf(
							"Aborting theft of stream '%v' shard '%v' from client '%v' as client '%v'\n",
							streamID, randomShard, clientID, k.clientID,
						)
						continue
					}

					k.log.Debugf(
						"Successfully stole stream '%v' shard '%v' from client '%v' as client '%v'\n",
						streamID, randomShard, clientID, k.clientID,
					)
					wg.Add(1)
					if err = k.runConsumer(&wg, streamID, randomShard, sequence); err != nil {
						k.log.Errorf("Failed to start consumer: %v\n", err)
					} else {
						// If we successfully stole the shard then that's enough
						// for now.
						break
					}
				}
			}
		}

		select {
		case <-time.After(k.rebalancePeriod):
		case <-k.ctx.Done():
			return
		}
	}
}

func (k *kinesisReader) runExplicitShards() {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		k.closeOnce.Do(func() {
			close(k.msgChan)
			close(k.closedChan)
		})
	}()

	for {
		for streamID, shards := range k.streamShards {
			var failedShards []string
			for _, shardID := range shards {
				sequence, err := k.checkpointer.Claim(k.ctx, streamID, shardID, "")
				if err == nil {
					wg.Add(1)
					err = k.runConsumer(&wg, streamID, shardID, sequence)
				}
				if err != nil {
					if k.ctx.Err() != nil {
						return
					}
					failedShards = append(failedShards, shardID)
					k.log.Errorf("Failed to start stream '%v' shard '%v' consumer: %v\n", streamID, shardID, err)
				}
			}
			if len(failedShards) > 0 {
				k.streamShards[streamID] = failedShards
			} else {
				delete(k.streamShards, streamID)
			}
		}
		if len(k.streamShards) == 0 {
			break
		} else {
			<-time.After(time.Second)
		}
	}
}

func (k *kinesisReader) waitUntilStreamsExists(ctx context.Context) error {
	streams := append([]string{}, k.balancedStreams...)
	for k := range k.streamShards {
		streams = append(streams, k)
	}

	results := make(chan error, len(streams))
	for _, s := range streams {
		go func(stream string) {
			results <- k.svc.WaitUntilStreamExistsWithContext(ctx, &kinesis.DescribeStreamInput{
				StreamName: &stream,
			})
		}(s)
	}

	for i := 0; i < len(streams); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// Connect establishes a kinesisReader connection.
func (k *kinesisReader) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	svc := kinesis.New(k.sess)
	checkpointer, err := newAWSKinesisCheckpointer(k.sess, k.clientID, k.conf.DynamoDB, k.leasePeriod, k.commitPeriod)
	if err != nil {
		return err
	}

	k.svc = svc
	k.checkpointer = checkpointer
	k.msgChan = make(chan asyncMessage)

	if err = k.waitUntilStreamsExists(ctx); err != nil {
		return err
	}

	if len(k.streamShards) > 0 {
		go k.runExplicitShards()
	} else {
		go k.runBalancedShards()
	}

	return nil
}

// ReadBatch attempts to read a message from Kinesis.
func (k *kinesisReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	k.cMut.Lock()
	msgChan := k.msgChan
	k.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
	}
	return nil, nil, component.ErrTimeout
}

// CloseAsync shuts down the Kinesis input and stops processing requests.
func (k *kinesisReader) Close(ctx context.Context) error {
	k.done()
	select {
	case <-k.closedChan:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
