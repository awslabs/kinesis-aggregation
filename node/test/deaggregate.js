// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
const assert = require('assert')
const sinon = require('sinon');
const should = require('should')
require('should-sinon')

const deaggregate = require('../index').deaggregate


describe('#deaggregate', () => {

    it('deaggregate is a function', () => {
        deaggregate.should.be.Function()
    })

})
