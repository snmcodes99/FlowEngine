/**
 * MongoDB connection module.
 * Uses the centralized config for connection URI.
 * Returns a promise that resolves when connected or rejects on failure.
 */
const mongoose = require('mongoose');
const config = require('./index');

const ConnectToDB = async () => {
  try {
    await mongoose.connect(config.mongo.uri);
    console.log(`Connected to MongoDB at ${config.mongo.uri.replace(/\/\/.*@/, '//***@')}`);
  } catch (error) {
    console.error('Failed to connect to MongoDB:', error.message);
    throw error;
  }
};

module.exports = ConnectToDB;
