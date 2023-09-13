import Yargs from 'yargs/yargs'
import { hideBin } from 'yargs/helpers'
import { argv as pargv } from 'process'
import {KubeConfig, CustomObjectsApi, V1ObjectMeta, CoreV1Api, CoreV1Event, CoreV1EventList, V1Pod, V1ContainerState, V1ContainerStatus} from '@kubernetes/client-node'
import { readFileSync } from 'fs'

const FixedElapseTime   = 1000000;
const CATPipelinerun    = 'PipelineRun';
const CATRun            = 'Run';
const CATTaskRun        = 'TaskRun';
const CATStep           = 'Step';
const CATContainer      = 'Container';
const CATInitContainer  = 'InitContainer';
const CATPodEvent       = 'PodEvent';
const OffSetStartTime   = false;
const IncludePodEvents  = false;
const NameSpace         = 'kubeflow'

var padding = FixedElapseTime;
var namespace = NameSpace;

type Argv = {
    pipelinerun?: string;
    filename?: string;
    padding?: string;
    namespace?: string;
};

type PipelineRunSpec = {
    pipelineSpec: Object;
};

type Condition = {
    lastTransitionTime: string;
    message: string;
    reason: string;
    status: string;
    type: string;
};

type RunStatus = {
    startTime: string;
    completionTime: string;
    conditions: Condition[];
};

type RunObj = {
    pipelineTaskName: string;
    status: RunStatus;
};

type Runs = {
    [name: string]: RunObj;
};

type TaskRunStep = {
    container: string;
    imageID: string;
    name: string,
    terminated: {
        containerID: string
        exitCode: number;
        finishedAt: string;
        message: string;
        reason: string,
        startedAt: string
    }
};

type TaskRunStatus = {
    startTime: string;
    completionTime: string;
    conditions: Condition[];
    podName: string;
    steps: TaskRunStep[]
}

type TaskRunObj = {
    pipelineTaskName: string;
    status: TaskRunStatus;
};

type TaskRuns = {
    [name: string]: TaskRunObj
};

type PipelineRunStatus = {
    startTime: string;
    completionTime: string;
    conditions: Condition[];
    runs: Runs;
    taskRuns: TaskRuns;
};

type PipelineRun = {
    apiVersion: string;
    kind: string;
    metadata: V1ObjectMeta;
    spec: PipelineRunSpec;
    status: PipelineRunStatus;
};

type PipelineRunList = {
    apiVersion: string;
    kind: string;
    metadata: V1ObjectMeta;
    items: PipelineRun[];
};

type TraceObj = {
    name: string;
    cat: string;
    ph: string;
    pid: number;
    tid: number;
    ts: number;
};

function getK8sApiClient() : CustomObjectsApi {
    const kc = new KubeConfig();
    kc.loadFromDefault();
    return kc.makeApiClient(CustomObjectsApi);
}

function getK8sEventClient() : CoreV1Api {
    const kc = new KubeConfig();
    kc.loadFromDefault();
    return kc.makeApiClient(CoreV1Api)
}

async function getPodEvents(client: CoreV1Api, podname: string): Promise<CoreV1EventList> {
    const res = await client.listNamespacedEvent(namespace, undefined, undefined, undefined, `involvedObject.name=${podname}`)
    return res.body
}

async function getPodStatus(client: CoreV1Api, podname: string): Promise<V1Pod> {
    const res = await client.readNamespacedPodStatus(podname, namespace);
    return res.body
}

async function parseStatus(status: PipelineRunStatus) : Promise<TraceObj[]> {
    const k8sClient = getK8sEventClient(); 
    if (status == undefined) {
        throw new Error('Incorrect PipelineRun status');
    }

    var tCounter = 1;
    var rev: TraceObj[] = [];
    const pStart = new Date(status.startTime)
    // the overall pipelinerun
    rev.push({
        name: 'PipelineRun',
        cat: CATPipelinerun,
        ph: 'B',
        pid: 1,
        tid: 1,
        ts: OffSetStartTime ? 0 : (new Date(status.startTime)).getTime() * 1000
    });
    rev.push({
        name: 'PipelineRun',
        cat: CATPipelinerun,
        ph: 'E',
        pid: 1,
        tid: 1,
        ts: elapse(pStart, new Date(status.completionTime))
    });

    // runs
    for (const runid in status.runs) {
        const run = status.runs[runid];
        var delta = 0;
        const rT = ++tCounter;
        rev.push({
            name: run.pipelineTaskName,
            cat: CATRun,
            ph: 'B',
            pid: 1,
            tid: rT,
            ts: elapse(pStart, new Date(run.status.startTime))
        });
        if (run.status.startTime === run.status.completionTime) {
            delta = padding;
        }
        rev.push({
            name: run.pipelineTaskName,
            cat: CATRun,
            ph: 'E',
            pid: 1,
            tid: rT,
            ts: elapse(pStart, new Date(run.status.completionTime)) + delta
        });
    }

    // taskRuns
    for (const name in status.taskRuns) {
        const taskrun = status.taskRuns[name];
        const rT = ++tCounter;
        rev.push({
            name: taskrun.pipelineTaskName,
            cat: CATTaskRun,
            ph: 'B',
            pid: 1,
            tid: rT,
            ts: elapse(pStart, new Date(taskrun.status.startTime))
        });
        //Pod Events
        if (IncludePodEvents) {
            const events = await getPodEvents(k8sClient, taskrun.status.podName)
            addPodEvent(rev, events.items, rT, pStart);
        }

        //Pod Status
        const pod = await getPodStatus(k8sClient, taskrun.status.podName);
        if (pod && pod.status) {
            addContainerStatus(rev, pod.status?.initContainerStatuses, `${CATInitContainer}`, rT, pStart);
            // addContainerStatus(rev, pod.status?.containerStatuses, `${CATContainer}`, rT, pStart);
        }

        taskrun.status.steps.forEach((step: TaskRunStep) => {
            var delta = 0;
            rev.push({
                name: `${taskrun.pipelineTaskName}-${step.name}`,
                cat: `${CATTaskRun},${CATStep}`,
                ph: 'B',
                pid: 1,
                tid: rT,
                ts: elapse(pStart, new Date(step.terminated.startedAt))
            });
            if (step.terminated.startedAt === step.terminated.finishedAt) {
                delta = padding;
            }
            rev.push({
                name: `${taskrun.pipelineTaskName}-${step.name}`,
                cat: `${CATTaskRun},${CATStep}`,
                ph: 'E',
                pid: 1,
                tid: rT,
                ts: elapse(pStart, new Date(step.terminated.finishedAt)) + delta
            });
        });

        var delta = 0;
        if (taskrun.status.startTime === taskrun.status.completionTime) {
            delta = padding;
        }
        rev.push({
            name: taskrun.pipelineTaskName,
            cat: CATTaskRun,
            ph: 'E',
            pid: 1,
            tid: rT,
            ts: elapse(pStart, new Date(taskrun.status.completionTime)) + delta
        });
    }


    return rev;
}

// Adding trace event for each pod event
function addPodEvent(traces: TraceObj[], events: CoreV1Event[], threadId: number, startTime: Date) {
    if (!events) {
        return;
    }
    events.forEach((event: CoreV1Event) => {
        var delta = 0;
        if (event.firstTimestamp == undefined || event.lastTimestamp == undefined) {
            return;
        }
        traces.push({
            name: event.message || 'event',
            cat: `${CATTaskRun},${CATPodEvent}`,
            ph: 'B',
            pid: 1,
            tid: threadId,
            ts: elapse(startTime, new Date(event.firstTimestamp))
        });
        if (event.firstTimestamp.getTime() === event.lastTimestamp.getTime()) {
            delta = padding;
        }
        traces.push({
            name: event.message || 'event',
            cat:  `${CATTaskRun},${CATPodEvent}`,
            ph: 'E',
            pid: 1,
            tid: threadId,
            ts: elapse(startTime, new Date(event.lastTimestamp)) + delta
        });
    });
}

// Adding trace event for each containter status
function addContainerStatus(traces: TraceObj[], cStatus: V1ContainerStatus[]|undefined, cat: string, threadId: number, startTime: Date) {
    if (!cStatus) {
        return;
    }
    cStatus.forEach((status: V1ContainerStatus) => {
        const sT = status.state?.terminated?.startedAt;
        const eT = status.state?.terminated?.finishedAt;
        if (sT && eT) {
            var delta = 0;
            traces.push({
                name: status.name,
                cat,
                ph: 'B',
                pid: 1,
                tid: threadId,
                ts: elapse(startTime, sT)
            });
            if (sT.getTime() === eT.getTime()) {
                delta = padding;
            }
            traces.push({
                name: status.name,
                cat,
                ph: 'E',
                pid: 1,
                tid: threadId,
                ts: elapse(startTime, eT) + delta
            });
        }
    });
}

function elapse(start: Date, end: Date) : number {
    if (OffSetStartTime) {
        return (end.getTime() - start.getTime()) * 1000
    } else {
        return end.getTime() * 1000
    }
}

async function getPipelineRun(kClient: CustomObjectsApi, name: string): Promise<PipelineRun|undefined> {
    const res = await kClient.getNamespacedCustomObject('tekton.dev', 'v1beta1', namespace,'pipelineruns', name);
    const pipelinerun = (res.body as PipelineRun);
    return await Promise.resolve(pipelinerun);
}

async function getPipelineRuns(kClient: CustomObjectsApi): Promise<PipelineRun[]|undefined> {
    const res = await kClient.listNamespacedCustomObject('tekton.dev', 'v1beta1', namespace, 'pipelineruns');
    const pipelineruns = (res.body as PipelineRunList).items as PipelineRun[];
    return await Promise.resolve(pipelineruns);
}

function run(argv: Argv) {
    const k8sClient = getK8sApiClient();
    if (argv.filename != undefined) {
        const pipelinerun = JSON.parse(readFileSync(argv.filename).toString('utf8')) as PipelineRun;
        parseStatus(pipelinerun.status).then((traceObjs) => {
            console.log(JSON.stringify(traceObjs, null, 2));
        });
    } else if (argv.pipelinerun != undefined) {
        getPipelineRun(k8sClient, argv.pipelinerun).then((pipelinerun) => {
            if (pipelinerun != undefined) {
                parseStatus(pipelinerun.status).then((traceObjs) => {
                    console.log(JSON.stringify(traceObjs, null, 2));
                });
            }
        })
    }
}

function main() {
    const yargv = Yargs(hideBin(pargv))
    .usage('Usage: $0 --pipelinerun [pipelinerun] --filename [file] --padding [number]')
    .describe('pipelinerun', 'pipelinerun name')
    .describe('filename', 'pipelinerun json file')
    .describe('padding', 'add mS for those events have the same start and end times. default is 1000000 mS (1 second)')
    .describe('namespace', 'namespace for the pipelinerun');
    
    const argv = yargv.argv as Argv;
    
    if (argv.filename === undefined && argv.pipelinerun === undefined) {
        yargv.showHelp()
        return;
    }
    
    if (argv.padding !=undefined) {
        padding = parseInt(argv.padding);
    }

    if (argv.namespace != undefined) {
        // for filename case, maybe get the namespace from the pipelinerun json
        namespace = argv.namespace;
    }

    run(argv)
} 


main()
