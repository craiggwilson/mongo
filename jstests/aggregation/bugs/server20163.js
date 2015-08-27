// SERVER-20163: Add $zip aggregation expression.

// For assertErrorCode.
load('jstests/aggregation/extras/utils.js');

(function() {
    'use strict';

    var coll = db.agg_zip_expr;
    coll.drop();

    assert.writeOK(coll.insert({_id: 0, a: [1,2,3,4,5], b: [1,2,3,4,5]}));
    assert.writeOK(coll.insert({_id: 1, a: [1,2], b: [1,2,3]}));
    assert.writeOK(coll.insert({_id: 2, a: [1,2,3], b: [1,2]}));
    assert.writeOK(coll.insert({_id: 3, a: [], b: [1]}));
    assert.writeOK(coll.insert({_id: 4, a: [1], b: []}));
    assert.writeOK(coll.insert({_id: 5, a: null, b: [1,2]}));
    assert.writeOK(coll.insert({_id: 6, a: [1,2], b: null}));
    assert.writeOK(coll.insert({_id: 7, a: [1,2]}));
    assert.writeOK(coll.insert({_id: 8, b: [1,2]}));
    assert.writeOK(coll.insert({_id: 9}));
    assert.writeOK(coll.insert({_id: 10, a: null, b: null}));
    assert.writeOK(coll.insert({_id: 11, a: undefined, b: undefined}));

    // Add the two values together
    var zipDoc = {input: ['$a', '$b'], as: ['x', 'y'], in: { $add: ['$$x', '$$y']}};
    var expectedResults = [
        {_id: 0, c: [2,4,6,8,10]},
        {_id: 1, c: [2, 4]},
        {_id: 2, c: [2, 4]},
        {_id: 3, c: []},
        {_id: 4, c: []},
        {_id: 5, c: null},
        {_id: 6, c: null},
        {_id: 7, c: null},
        {_id: 8, c: null},
        {_id: 9, c: null},
        {_id: 10, c: null},
        {_id: 11, c: null}
    ];
    var results = coll.aggregate([{$project: {c: {$zip: zipDoc}}}]).toArray();
    assert.eq(results, expectedResults);


    // Invalid zip expressions.

    // not a document.
    var zipDoc = 'string';
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28900);

    // bad parameter.
    var zipDoc = { input: ['$a','$b'], as: ['a','b'], in: { $add: ['$$a','$$b'] }, extra: 1 };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28901);

    // missing input
    var zipDoc = { as: ['a','b'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28902);

    // missing as
    var zipDoc = { input: ['$a','$b'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28903);

    // missing in
    var zipDoc = { input: ['$a','$b'], as: ['a','b'] };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28904);

    // input is not an array
    var zipDoc = { input: '$a', as: ['a','b'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28905);

    // as is not an array
    var zipDoc = { input: ['$a','$b'], as: 'a', in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28906);

    // as has too little parameters
    var zipDoc = { input: ['$a','$b'], as: ['a'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28907);

    // as has too many parameters
    var zipDoc = { input: ['$a','$b'], as: ['a','b','c'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28907);

    // input has too little values
    var zipDoc = { input: ['$a'], as: ['a','b'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28908);

    // input has too many values
    var zipDoc = { input: ['$a', '$b', '$c'], as: ['a','b'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28908);

    // first input value is not an array
    var zipDoc = { input: ['$_id', '$b'], as: ['a','b'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28909);

    // second input value is not an array
    var zipDoc = { input: ['$a', '$_id'], as: ['a','b'], in: { $add: ['$$a','$$b'] } };
    assertErrorCode(coll, [{$project: {b: {$zip: zipDoc}}}], 28910);
}());