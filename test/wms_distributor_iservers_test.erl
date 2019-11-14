%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%%
%%% @end
%%% Created : 09. Nov 2019 04:49
%%%-------------------------------------------------------------------
-module(wms_distributor_iservers_test).
-author("Attila Makra").

-include_lib("eunit/include/eunit.hrl").

%% -----------------------------------------------------------------------------

new_test() ->
  Instance = wms_distributor_iservers:new(),
  ?assertEqual(#{
                 interactions => #{},
                 nodes => #{}}, Instance).

%% -----------------------------------------------------------------------------

login_logout_test() ->
  Instance = wms_distributor_iservers:new(),

  % login node1

  Node1 = node1,
  Interactions1 = [<<"I1">>, <<"I2">>],

  Expected1 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result1 = wms_distributor_iservers:login(Node1, Interactions1, Instance),
  ?assertMatch(Expected1, Result1),

  % login node2

  Node2 = node2,
  Interactions2 = [<<"I2">>, <<"I3">>],

  Expected2 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node2, Node1],
                  <<"I3">> => [Node2]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  },
                  Node2 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result2 = wms_distributor_iservers:login(Node2, Interactions2, Result1),
  ?assertEqual(Expected2, Result2),

  % login node2 again

  Interactions21 = [<<"I2">>, <<"I3">>],

  Expected21 = #{
                 interactions => #{
                   <<"I1">> => [Node1],
                   <<"I2">> => [Node2, Node1],
                   <<"I3">> => [Node2]
                 },
                 nodes => #{
                   Node1 => #{
                     status => up,
                     last_run => undefined,
                     running => #{}
                   },
                   Node2 => #{
                     status => up,
                     last_run => undefined,
                     running => #{}
                   }
                 }
               },
  Result21 = wms_distributor_iservers:login(Node2, Interactions21, Result2),
  ?assertEqual(Expected21, Result21),

  % logout node1

  Expected3 = #{
                interactions => #{
                  <<"I2">> => [Node2],
                  <<"I3">> => [Node2]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  },
                  Node2 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result3 = wms_distributor_iservers:logout(Node1, Result21),
  ?assertEqual(Expected3, Result3),

  % logout nodexxx which was not logged in

  Expected4 = #{
                interactions => #{
                  <<"I2">> => [Node2],
                  <<"I3">> => [Node2]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  },
                  Node2 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result4 = wms_distributor_iservers:logout(nodexxx, Result3),
  ?assertEqual(Expected4, Result4),

  % logout node2

  Expected5 = #{
                interactions => #{
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  },
                  Node2 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result5 = wms_distributor_iservers:logout(Node2, Result4),
  ?assertEqual(Expected5, Result5),

  ok.

%% -----------------------------------------------------------------------------

node_status_test() ->
  try
    meck:new(wms_common, [passthrough]),
    meck:expect(wms_common, timestamp,
                fun() ->
                  now
                end),
    node_status_test_impl()
  after
    meck:unload(wms_common)
  end.

node_status_test_impl() ->
  Instance = wms_distributor_iservers:new(),

  % login node1

  Node1 = node1,
  Interactions1 = [<<"I1">>, <<"I2">>],

  Expected1 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result1 = wms_distributor_iservers:login(Node1, Interactions1, Instance),
  ?assertMatch(Expected1, Result1),

  % node1 up
  Expected2 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  {[], Result2} = wms_distributor_iservers:up(Node1, Result1),
  ?assertMatch(Expected2, Result2),

  % node1 down
  Expected3 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => down,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  {[], Result3} = wms_distributor_iservers:down(Node1, Result2),
  ?assertMatch(Expected3, Result3),

  % node1 up again

  Expected4 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  {[], Result4} = wms_distributor_iservers:up(Node1, Result3),
  ?assertMatch(Expected4, Result4),

  % add interactions to node1
  Result5 = wms_distributor_iservers:start_interaction(Node1,
                                                       <<"I1_RQ1">>,
                                                       <<"TI_1">>,
                                                       <<"I1">>,
                                                       Result4),

  Expected5 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_RQ1">> => {now, <<"TI_1">>, <<"I1">>}
                    }
                  }
                }
              },
  ?assertMatch(Expected5, Result5),

  % node1 down, interaction will be removed

  Expected6 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => down,
                    last_run => now,
                    running => #{}
                  }
                }
              },
  {[Broken], Result6} = wms_distributor_iservers:down(Node1, Result5),
  ?assertMatch(Expected6, Result6),
  ?assertEqual(#{task_instance_id => <<"TI_1">>,
                 interaction_id => <<"I1">>,
                 interaction_request_id => <<"I1_RQ1">>}, Broken),
  ok.

%% -----------------------------------------------------------------------------

interaction_test() ->
  try
    meck:new(wms_common, [passthrough]),
    meck:expect(wms_common, timestamp,
                fun() ->
                  now
                end),
    interaction_test_impl()
  after
    meck:unload(wms_common)
  end.

interaction_test_impl() ->
  Instance = wms_distributor_iservers:new(),

  % login node1

  Node1 = node1,
  Interactions1 = [<<"I1">>, <<"I2">>],

  Expected1 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result1 = wms_distributor_iservers:login(Node1, Interactions1, Instance),
  ?assertMatch(Expected1, Result1),

  % add interactions I1_RQ1 to node1
  Result2 = wms_distributor_iservers:start_interaction(Node1,
                                                       <<"I1_RQ1">>,
                                                       <<"TI_1">>,
                                                       <<"I1">>,
                                                       Result1),

  Expected2 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_RQ1">> => {now, <<"TI_1">>, <<"I1">>}
                    }
                  }
                }
              },
  ?assertMatch(Expected2, Result2),

  % get interaction data
  ?assertEqual({<<"TI_1">>, <<"I1">>},
               wms_distributor_iservers:get_interaction_data(Node1,
                                                             <<"I1_RQ1">>,
                                                             Result2)),
  ?assertEqual(undefined,
               wms_distributor_iservers:get_interaction_data(inv_node,
                                                             <<"I1_RQ1">>,
                                                             Result2)),
  ?assertEqual(undefined,
               wms_distributor_iservers:get_interaction_data(Node1,
                                                             <<"inv_rq1">>,
                                                             Result2)),

  % add interactions I2_RQ2 to node1
  Result3 = wms_distributor_iservers:start_interaction(Node1,
                                                       <<"I2_RQ2">>,
                                                       <<"TI_2">>,
                                                       <<"I2">>,
                                                       Result2),

  Expected3 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_RQ1">> => {now, <<"TI_1">>, <<"I1">>},
                      <<"I2_RQ2">> => {now, <<"TI_2">>, <<"I2">>}
                    }
                  }
                }
              },
  ?assertMatch(Expected3, Result3),

  % end interaction I2_RQ2

  Result4 = wms_distributor_iservers:stop_interaction(Node1, <<"I2_RQ2">>, Result3),

  Expected4 = {{<<"TI_2">>, <<"I2">>},
               #{
                 interactions => #{
                   <<"I1">> => [Node1],
                   <<"I2">> => [Node1]
                 },
                 nodes => #{
                   Node1 => #{
                     status => up,
                     last_run => now,
                     running => #{
                       <<"I1_RQ1">> => {now, <<"TI_1">>, <<"I1">>}
                     }
                   }
                 }
               }},
  ?assertMatch(Expected4, Result4),

  {_, Result41} = Result4,

  % end interaction I1_RQ1 from unknown_node node
  Result5 = wms_distributor_iservers:stop_interaction(unknown_node,
                                                      <<"I1_RQ1">>, Result41),

  Expected5 = {undefined,
               #{
                 interactions => #{
                   <<"I1">> => [Node1],
                   <<"I2">> => [Node1]
                 },
                 nodes => #{
                   Node1 => #{
                     status => up,
                     last_run => now,
                     running => #{
                       <<"I1_RQ1">> => {now, <<"TI_1">>, <<"I1">>}
                     }
                   }
                 }
               }},
  ?assertMatch(Expected5, Result5),

  ok.

%% -----------------------------------------------------------------------------

get_actor_node_test() ->
  try
    meck:new(wms_common, [passthrough]),
    meck:expect(wms_common, timestamp,
                fun() ->
                  now
                end),
    get_actor_node_test_impl()
  after
    meck:unload(wms_common)
  end.

get_actor_node_test_impl() ->
  Instance = wms_distributor_iservers:new(),

  % login node1

  Node1 = node1,
  Interactions1 = [<<"I1">>, <<"I2">>],

  Expected1 = #{
                interactions => #{
                  <<"I1">> => [Node1],
                  <<"I2">> => [Node1]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result1 = wms_distributor_iservers:login(Node1, Interactions1, Instance),
  ?assertMatch(Expected1, Result1),

  % login node2

  Node2 = node2,
  Interactions2 = [<<"I1">>, <<"I3">>],

  Expected2 = #{
                interactions => #{
                  <<"I1">> => [Node2, Node1],
                  <<"I2">> => [Node1],
                  <<"I3">> => [Node2]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  },
                  Node2 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  }
                }
              },
  Result2 = wms_distributor_iservers:login(Node2, Interactions2, Result1),
  ?assertMatch(Expected2, Result2),

  % get actor node for I1 interactions
  Node2 = wms_distributor_iservers:get_actor_node(<<"I1">>, Result2),
  % get actor node for I2 interactions
  Node1 = wms_distributor_iservers:get_actor_node(<<"I2">>, Result2),
  % get actor node for I3 interactions
  Node2 = wms_distributor_iservers:get_actor_node(<<"I3">>, Result2),
  % get actor node for I4 interactions
  undefined = wms_distributor_iservers:get_actor_node(<<"I4">>, Result2),

  % start interaction I1 on node2

  Expected3 = #{
                interactions => #{
                  <<"I1">> => [Node2, Node1],
                  <<"I2">> => [Node1],
                  <<"I3">> => [Node2]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => undefined,
                    running => #{}
                  },
                  Node2 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_1">> => {now, <<"T1">>, <<"I1">>}

                    }
                  }
                }
              },

  Result3 = wms_distributor_iservers:start_interaction(Node2,
                                                       <<"I1_1">>,
                                                       <<"T1">>,
                                                       <<"I1">>, Result2),
  ?assertEqual(Expected3, Result3),

  % get actor node for I1 interactions, Node2 has an interaction already
  Node1 = wms_distributor_iservers:get_actor_node(<<"I1">>, Result3),


  % start interaction I1 on node1

  Expected4 = #{
                interactions => #{
                  <<"I1">> => [Node2, Node1],
                  <<"I2">> => [Node1],
                  <<"I3">> => [Node2]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_2">> => {now, <<"T2">>, <<"I1">>}
                    }
                  },
                  Node2 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_1">> => {now, <<"T1">>, <<"I1">>}

                    }
                  }
                }
              },

  Result4 = wms_distributor_iservers:start_interaction(Node1,
                                                       <<"I1_2">>,
                                                       <<"T2">>,
                                                       <<"I1">>, Result3),
  ?assertEqual(Expected4, Result4),

  % get actor node for I1 interactions, Node1 and Node2 have one interaction
  Node2 = wms_distributor_iservers:get_actor_node(<<"I1">>, Result4),

  % start interaction I3 on node2

  Expected5 = #{
                interactions => #{
                  <<"I1">> => [Node2, Node1],
                  <<"I2">> => [Node1],
                  <<"I3">> => [Node2]
                },
                nodes => #{
                  Node1 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_2">> => {now, <<"T2">>, <<"I1">>}
                    }
                  },
                  Node2 => #{
                    status => up,
                    last_run => now,
                    running => #{
                      <<"I1_1">> => {now, <<"T1">>, <<"I1">>},
                      <<"I3_1">> => {now, <<"T3">>, <<"I3">>}
                    }
                  }
                }
              },

  Result5 = wms_distributor_iservers:start_interaction(Node2,
                                                       <<"I3_1">>,
                                                       <<"T3">>,
                                                       <<"I3">>, Result4),
  ?assertEqual(Expected5, Result5),

  % get actor node for I2 interaction again
  Node2 = wms_distributor_iservers:get_actor_node(<<"I3">>, Result5),

  ok.