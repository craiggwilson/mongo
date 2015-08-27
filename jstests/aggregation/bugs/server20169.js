// SERVER-20169: Add $range aggregation expression.

// For assertErrorCode.
load('jstests/aggregation/extras/utils.js');

(function() {
    'use strict';

    var coll = db.agg_range_expr;
    coll.drop();

    assert.writeOK(coll.insert({_id: 0, a: 0, b: 5, c: 2}));
    assert.writeOK(coll.insert({_id: 1, a: 1, b: 5, c: 2.2}));

    // without step
    var expectedResults = [
        { _id: 0, a: [0,1,2,3,4] },
        { _id: 1, a: [1,2,3,4,5] },
    ];
    var results = coll.aggregate([{$project: {a: {$range: ['$a','$b']}}}]).toArray();
    assert.eq(results, expectedResults);

    // with step
    var expectedResults = [
        { _id: 0, a: [0,2,4,6,8] },
        { _id: 1, a: [1,3.2,5.4,7.6000000000000005,9.8] }, // weird rounding only happens on the 4th number.
    ];
    var results = coll.aggregate([{$project: {a: {$range: ['$a','$b','$c']}}}]).toArray();
    assert.eq(results, expectedResults);


    // Invalid range expressions.

    // '$range' is not an array.
    assertErrorCode(coll, [{$project: {a: { $range: 10 }}}], 28667);

    // '$range' has too little arguments
    assertErrorCode(coll, [{$project: {a: { $range: [0] }}}], 28667);

    // '$range' has too many arguments
    assertErrorCode(coll, [{$project: {a: { $range: [0,1,2,3] }}}], 28667);

    // '$range' first argument is not a number
    assertErrorCode(coll, [{$project: {a: { $range: ['a',1] }}}], 29050);

    // '$range' second argument is not a number
    assertErrorCode(coll, [{$project: {a: { $range: [0,'a'] }}}], 29051);

    // '$range' second argument is not an integer
    assertErrorCode(coll, [{$project: {a: { $range: [0,1.1] }}}], 29052);

    // '$range' third argument is not a number
    assertErrorCode(coll, [{$project: {a: { $range: [0,10,'a'] }}}], 29053);
}());