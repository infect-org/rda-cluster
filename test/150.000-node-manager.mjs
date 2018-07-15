'use strict';

import NodeManager from '../src/NodeManager';
import Service from '../';
import {ServiceManager} from 'rda-service';
import section from 'section-tests';
import assert from 'assert';
import log from 'ee-log';



const host = 'http://l.dns.porn';



section('NodeManager', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
    });



    section.test('Create Instance', async () => {
        new NodeManager({});
    });




    section.test('Async Queue', async () => {
        const manager = new NodeManager({});

        let result = '';


        // inject test methods
        manager.test = function(...args) {
            return this.enqueue({
                method: '_test',
                args: args
            });
        };
        

        manager._test = (input, msecs) => {
            return new Promise((resolve) => {
                setTimeout(() => {
                    result += input;
                    resolve();
                }, msecs);
            });
        }


        await Promise.all([
            manager.test('first', 200),
            manager.test('second', 20),
        ]);


        assert.equal(result, 'firstsecond');
    });





    section.test('Locks', async () => {
        const manager = new NodeManager({});

        let result = '';

        const doA = () => {
            return new Promise((resolve) => {
                setTimeout(() => {
                    result += 'first';
                    resolve();
                }, 200)
            });
        }

        const doB = () => {
            return new Promise((resolve) => {
                setTimeout(() => {
                    result += 'second';
                    resolve();
                }, 20)
            });
        }


        await Promise.all([
            manager.lock(doA),
            manager.lock(doB),
        ]);


        assert.equal(result, 'firstsecond');
    });







    section.test('Sync instances', async () => {
        const service = new Service();
        await service.load();


        const manager = new NodeManager({
            db: service.db
        });


        await manager.syncInstances([{
            identifier: 'instance'+Math.round(Math.random()*10000000),
            availableMemory: 342543,
            ipv4address: 'invalid',
            machineId: 'server-1',
        }]);


        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});