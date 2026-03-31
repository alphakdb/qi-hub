/ Hub

.qi.import`ipc
.qi.import`cron

ws.push:{[h;x] neg[(),h]@\:.j.j`callback`result!x;}
ws.pushall:{if[count h:where"w"=k!exec p from -38!k:key .z.W;ws.push[h;x]]}
.z.ws:{a:.j.k x;r:@[get;a`cmd;{"kdb error: ",x}];if[not"none"~cb:a`callback;ws.push[.z.w;(cb;r)]]}    / cb=callback
pub:{[t;x] ws.pushall("upd";(t;x))}

/ ---- Start Public API Functions ----
writestack:{[st;x]
  .qi.info(`writestack;st);
  if[not first r:.qi.try[.j.k;raze x;0];
    '"stack json is badly formed: ",r 2];
  (p:.qi.path(.conf.STACKS;st;`stack.json))0:x;
  refresh[];
  p
  }

readstack:{[st] 
  .qi.info(`readstack;st);
  $[.qi.exists p:.qi.path(.conf.STACKS;st;`stack.json);read0 p;'.qi.spath[p]," not found"]
  }

writescript:{[filename;x]
  .qi.info(`writescript;filename);
  .qi.path[(.conf.SCRIPTS;filename)]0:x
  }

readscript:{[filename] 
  .qi.info(`readscript;filename);
  $[.qi.exists p:.qi.path(.conf.SCRIPTS;filename);read0 p;'.qi.spath[p]," not found"]
  }

readscripts:{(last each` vs'p)!read0 each p:.qi.paths[.conf.SCRIPTS;"*.q"]}

deletestack:{[st]
  .qi.info(`deletestack;st);
  if[count a:select from procs where stackname=st,status<>`down;
    show a;
    '"Cannot delete a stack with running processes"];
  if[not .qi.exists p:.qi.path(.conf.STACKS;st);
    '.qi.spath[p]," not found"];
  .qi.deldir p;
  refresh[]
  }

refreshlogs:{[st;nm]
  a:0!.hub.procinfo;
  if[not null st;a:select from a where stackname=st];
  if[count nm;a:select from a where name in nm];
  {$[.qi.exists p:x`logfile;`time`sym`stackname`lines!(.z.p;p;x`stackname;.proc.tail x`name);()]}each a
  }

clonestack:{[st;nst;port2]
  .qi.info(`clonestack;st;nst);
  if[.qi.exists targ:(.conf.STACKS;nst;`stack.json);
    '.qi.spath[targ]," already exists"];
  a:.qi.readj (.conf.STACKS;st;`stack.json);
  port:$[0^port2;port2;count s:1_.proc.stacks;1000+max get s[;`base_port];.conf.HUB_PORT];
  a[`base_port]:port;
  writestack[nst;.qi.formatj .j.j a];
  }

renamestack:{[st;nst]
  .qi.info(`renamestack;st;nst);
  if[count a:select from procs where stackname=st,status=`up;
    show a;
    '"Cannot rename a stack with running processes"];
  frm:.qi.path(.conf.STACKS;st);
  if[.qi.exists to:.qi.path(.conf.STACKS;nst);'.qi.spath[tp]," already exists"];
  .qi.os.mv[frm;to];
  refresh[];
  }
/ ------ End Public API functions

.hub.init:{
  .proc.self,:`name`stackname`fullname`subscribe_to!(`hub;`hub;`hub;());
  updprocs[];
  .cron.add[`check;0Np;.conf.HUB_CHECK_PERIOD];
  system"p ",.qi.tostr .conf.HUB_PORT;
  .proc.reporthealth[];
  monprocs[];
  .cron.start[];
  }

refresh:{
  .qi.info"refresh[]";
  .proc.loadstacks[];
  updprocs[];
  monprocs[];
  }

updprocs:{
   pr:1!select name,proc,stackname,port,status:`down,pid:0Ni,heartbeat:0Np,attempts:0N,lastattempt:0Np,lastattempt:0Np,used:0N,heap:0N,goal:` from .ipc.conns where proc<>`hub;
  `procs set $[`procs in tables`.;pr upsert select from procs where status<>`down;pr];
  a:select from .proc.getstacks[] where fullname in exec name from procs;
  .hub.procinfo:1!select name:fullname,stackname,logfile:.proc.getlog each fullname,depends_on:{[st;sub;pt] .proc.tofullnamex[;st]each key[sub]union pt}'[stackname;subscribe_to;publish_to]from a;
  }

/ monitor processes
monprocs:{
  .qi.import`mon;
  .mon.follow .'flip get exec stackname,logfile from .hub.procinfo;
  if[null .cron.jobs[f:`.mon.monitor]`period;.cron.add[f;0Np;.conf.MON_PERIOD]];
  }

getprocess:{[pname] $[null(x:procs pname)`proc;();x]}

/ process control functions
updown:{[cmd;x]
  .qi.info -3!(cmd;x);
  if[11<>abs t:type x;'"Require symbol name(s) of process/stack"];
  if[11=t;.z.s each x;:(::)];
  if[x in`all,as:1_key .proc.stacks;.z.s[cmd]each $[x=`all;as;.proc.stackprocs x];:(::)];
  if[null status:(e:procs nm:.proc.tofullname x)`status;'"invalid process name ",string nm];
  procs[nm;`goal]:cmd;
  if[status=cmd;:(::)];
  procs[nm],:select attempts:1+0^attempts,lastattempt:.z.p from e;
  .proc[cmd]nm;
  }

up:updown`up
down:updown`down
kill:.proc.kill

upall:{up each exec name from procs;}
downall:{down each exec name from procs;}

updAPI:{
  if[sub:count .z.W;pub[`processes;0!procs]];
  if[not count MonText;:()];
  if[sub;pub[`Logs;MonText lj `sym xkey select sym:logfile,name from .hub.procinfo]];
  delete from`MonText;
  }

check:{
  a:update health:{.proc.checkhealth .` vs x}each name from procs;
  a:update status:`down`up health[;0]from a;
  `procs set delete health from a,'exec health[;1]from a;
  update status:`busy from`procs where status=`up,not null heartbeat,heartbeat<.z.p-.conf.HUB_BUSY_PERIOD;
  if[count tostart:select from procs where goal=`up,status=`down,attempts<.conf.MAX_START_ATTEMPTS;
    if[count tostart:delete from tostart where not null lastattempt,.conf.HUB_ATTEMPT_PERIOD>.z.p-lastattempt;
      stilldown:exec name from procs where status=`down;
      tostart:tostart lj .hub.procinfo;
      up each exec name from tostart where 0=count each depends_on inter\:stilldown]];
  update attempts:0N from`procs where status=goal;
  updAPI[];
  }