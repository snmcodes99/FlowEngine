const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const Event = require('../../model/publishModel');
const DLQ = require('../../model/dlqModel');
const ConsumerCheckpoint = require('../../model/checkpointModel');
const StreamCounter = require('../../model/streamCounterModel');

describe('Mongoose Schema Validations', () => {
  describe('Publish Model', () => {
    it('should fail validation when required fields are missing', () => {
      const event = new Event({});
      const err = event.validateSync();
      assert.ok(err);
      assert.ok(err.errors.type, 'type should be required');
      assert.ok(err.errors.data, 'data should be required');
      assert.ok(err.errors.offset, 'offset should be required');
    });

    it('should pass validation with valid required fields', () => {
      const event = new Event({
        type: 'order.created',
        data: { orderId: '123' },
        offset: 1,
      });
      const err = event.validateSync();
      assert.equal(err, undefined);
      assert.ok(event.eventId, 'should generate a default UUID eventId');
    });
  });

  describe('DLQ Model', () => {
    it('should fail validation when required fields without defaults are missing', () => {
      const dlq = new DLQ({});
      const err = dlq.validateSync();
      assert.ok(err);
      assert.ok(err.errors.eventId);
      assert.ok(err.errors.type);
      assert.ok(err.errors.data);
      assert.ok(err.errors.offset);
      assert.equal(dlq.reason, 'Processing failed');
    });

    it('should pass validation with valid fields', () => {
      const dlq = new DLQ({
        eventId: '550e8400-e29b-41d4-a716-446655440000',
        type: 'order.created',
        data: { orderId: '123' },
        offset: 1,
        reason: 'Max retries exceeded',
      });
      const err = dlq.validateSync();
      assert.equal(err, undefined);
      assert.equal(dlq.status, 'pending');
    });
  });

  describe('Checkpoint Model', () => {
    it('should fail validation without consumerGroup and stream', () => {
      const cp = new ConsumerCheckpoint({});
      const err = cp.validateSync();
      assert.ok(err);
      assert.ok(err.errors.consumerGroup);
      assert.ok(err.errors.stream);
    });

    it('should pass validation with proper fields', () => {
      const cp = new ConsumerCheckpoint({
        consumerGroup: 'payment-processor',
        stream: 'payment.authorized',
        committedOffset: 42,
      });
      const err = cp.validateSync();
      assert.equal(err, undefined);
    });
  });

  describe('StreamCounter Model', () => {
    it('should fail validation without stream', () => {
      const counter = new StreamCounter({});
      const err = counter.validateSync();
      assert.ok(err);
      assert.ok(err.errors.stream);
    });

    it('should default lastOffset to 0', () => {
      const counter = new StreamCounter({ stream: 'payment.authorized' });
      assert.equal(counter.lastOffset, 0);
    });
  });
});
