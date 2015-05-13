var _ = require('underscore');

var BuffersHandler = module.exports = function(connectionObject, buffers, parentModelPk) {
    this.connectionObject = connectionObject;
    this.buffers = buffers;
    this.parentModelPk = parentModelPk;
};

BuffersHandler.prototype.createBuffer = function(parent, attribute, populationObject ) {
    var self = this;
    var popInstructions = populationObject.instructions;
    var strategy = populationObject.strategy.strategy;
    var alias = strategy === 1 ? popInstructions[0].parentKey : popInstructions[0].alias;
    var childCollection = strategy === 3 ? popInstructions[1].child : popInstructions[0].child;
    var childKey = strategy === 3 ? popInstructions[1].childKey : popInstructions[0].childKey;
    var buffer = {
        attrName: attribute,
        parentPK: /*parent[pk],*/parent[self.parentModelPk],
        pkAttr: /*pk,*/ self.parentModelPk,
        keyName: alias,
        childCollection: childCollection,
        childKey: childKey,
        strategy : strategy
    };
    return buffer;
};

BuffersHandler.prototype.addChildToBuffer = function(buffer, child) {
    if (!buffer.records)
        buffer.records = [];
    buffer.records.push(child);
    return buffer;
};

BuffersHandler.prototype.searchBufferAndAddChild = function(child, foreignKeyColumn, attributeToPopulate) {
    var self = this;
    var parentPkValueOfThisChild = child[foreignKeyColumn];
    var bufferOfThisChild = _.find(self.buffers.store, function(buffer) {
        return buffer.attrName === attributeToPopulate && buffer.belongsToPKValue === parentPkValueOfThisChild;
    });
    if (bufferOfThisChild) {
        if (!bufferOfThisChild.records)
            bufferOfThisChild.records = [];
        bufferOfThisChild.records.push(child);
    }
};

BuffersHandler.prototype.saveBuffer = function(buffer) {
    if(!this.buffers.store) this.buffers.store = [];
    this.buffers.store.push({
      attrName: buffer.attrName,
      parentPkAttr: buffer.pkAttr,
      records: buffer.records,
      keyName: buffer.keyName,
      belongsToPKValue: buffer.parentPK,
      childCollection: buffer.childCollection,
      childKey: buffer.childKey,
      strategy:buffer.strategy,
      // Optional (only used if implementing a HAS_FK strategy)
      belongsToFKValue: buffer.parentFK
    });
};

BuffersHandler.prototype.setParents = function(parents) {
    this.buffers.parents = parents;
};

