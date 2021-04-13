'use strict'

const debug = require('debug')('platziverse:mqtt')
// const aedes = require('aedes')()
const redis = require('redis')
const chalk = require('chalk')
const mosca = require('mosca')
const db = require('platziverse-db')
const {parsePayload} = require('./utils')

const backend = {
    type: 'redis',
    redis,
    return_buffers:true
}

const settings = {
    port: 1883,
    backend
}

const config = require('./../platziverse-db/config-db')(false)
const server = new mosca.Server(settings)
const clients = new Map()

let Agent,Metric

server.on('ready', async ()=>{
    const services = await db(config).catch(handleFatalError)

    Agent = services.Agent
    Metric = services.Metric
    console.log(chalk.green('platziverse-mqtt  server is running'))
})

server.on('clientConnected', client => {
    debug(`Client connected: ${client.id}`)
    clients.set(client.id, null)
})

server.on('clientDisconnected', async client =>{
    debug(`Client disconnected: ${client.id}`)
    const agent = clients.get(client.id)
    if(agent){
        // Mark Agent as Disconnected
        agent.connected = false
        try {
            await Agent.createOrUpdate(agent)
        } catch (e) {
            return handleError(e)
        }
        //Delete agent from clients list
        clients.delete(client.id)
        server.publish({
            topic:'agent/disconnected',
            payload: JSON.stringify({
                agent:{
                    uuid:agent.uuid
                }
            })
        })
        debug(`Client(${client.id}) associated to Agent (${agent.uuid}) marked as disconnected`)
    }
})

server.on('published',async(packet,client) =>{
    debug(`Received: ${packet.topic}`)

    switch (packet.topic) {
        case 'agent/connected':
        case 'agent/disconnected':
            debug(`Payload: ${packet.payload}`)
            break;
        case 'agent/message':
            debug(`Payload: ${packet.payload}`)
            const payload = parsePayload(packet.payload)
            if(payload){
                payload.agent.connected = true
                let agent 
                try {
                    agent = await Agent.createOrUpdate(payload.agent)
                } catch (error) {
                    return handleError(error)   
                }
                debug(`Agent: ${agent.uuid} saved`)
                //Notify Agent is connected
                if(!clients.get(client.id)){
                    clients.set(client.id,agent)
                    server.publish({
                        topic: 'agent/connected',
                        payload: JSON.stringify({
                            agent:{
                                uuid:agent.uuid,
                                name:agent.name,
                                hostname:agent.hostname,
                                pid:agent.pid,
                                connected:agent.connected
                            }
                        })
                    })
                }
                //store metrics
                for(let metric of payload.metrics){
                    let m
                    try {
                        m = await Metric.create(agent.uuid, metric)
                    } catch (e) {
                       return handleError(e)
                    }
                    debug(`Metric: ${m.id} saved on agent ${agent.uuid}`)
                }
            }
            break;
    }


})

server.on('error', handleFatalError)

function handleFatalError(err){
    console.error(chalk.red('[fatal error]', err.message))
    console.error(err.stack)
    process.exit(1)
}

function handleError(err){
    console.error(chalk.red('[fatal error]', err.message))
    console.error(err.stack)
}
process.on('uncaughtException', handleFatalError)
process.on('unhandledRejection', handleFatalError)

