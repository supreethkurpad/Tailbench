
// File: index.xml

// File: ps__lattice_8h.xml
%feature("docstring")  ps_lattice_read "

Read a lattice from a file on disk.

Parameters:
-----------

ps:  Decoder to use for processing this lattice, or NULL.

file:  Path to lattice file.

Newly created lattice, or NULL for failure. ";

%feature("docstring")  Lattice::retain "

Retain a lattice.

This function retains ownership of a lattice for the caller,
preventing it from being freed automatically. You must call
ps_lattice_free() to free it after having called this function.

pointer to the retained lattice. ";

%feature("docstring")  Lattice::free "

Free a lattice.

new reference count (0 if dag was freed) ";

%feature("docstring")  Lattice::write "

Write a lattice to disk.

0 for success, <0 on failure. ";

%feature("docstring")  Lattice::write_htk "

Write a lattice to disk in HTK format.

0 for success, <0 on failure. ";

%feature("docstring")  Lattice::get_logmath "

Get the log-math computation object for this lattice.

The log-math object for this lattice. The lattice retains ownership of
this pointer, so you should not attempt to free it manually. Use
logmath_retain() if you wish to reuse it elsewhere. ";

%feature("docstring")  Lattice::ps_latnode_iter "

Start iterating over nodes in the lattice.

No particular order of traversal is guaranteed, and you should not
depend on this.

Parameters:
-----------

dag:  Lattice to iterate over.

Iterator over lattice nodes. ";

%feature("docstring")  ps_latnode_iter_next "

Move to next node in iteration.

Parameters:
-----------

itor:  Node iterator.

Updated node iterator, or NULL if finished ";

%feature("docstring")  ps_latnode_iter_free "

Stop iterating over nodes.

Parameters:
-----------

itor:  Node iterator. ";

%feature("docstring")  ps_latnode_iter_node "

Get node from iterator. ";

%feature("docstring")  ps_latnode_times "

Get start and end time range for a node.

Parameters:
-----------

node:  Node inquired about.

out_fef:  Output: End frame of first exit from this node.

out_lef:  Output: End frame of last exit from this node.

Start frame for all edges exiting this node. ";

%feature("docstring")  Lattice::ps_latnode_word "

Get word string for this node.

Parameters:
-----------

dag:  Lattice to which node belongs.

node:  Node inquired about.

Word string for this node (possibly a pronunciation variant). ";

%feature("docstring")  Lattice::ps_latnode_baseword "

Get base word string for this node.

Parameters:
-----------

dag:  Lattice to which node belongs.

node:  Node inquired about.

Base word string for this node. ";

%feature("docstring")  ps_latnode_exits "

Iterate over exits from this node.

Parameters:
-----------

node:  Node inquired about.

Iterator over exit links from this node. ";

%feature("docstring")  ps_latnode_entries "

Iterate over entries to this node.

Parameters:
-----------

node:  Node inquired about.

Iterator over entry links to this node. ";

%feature("docstring")  Lattice::ps_latnode_prob "

Get best posterior probability and associated acoustic score from a
lattice node.

Parameters:
-----------

dag:  Lattice to which node belongs.

node:  Node inquired about.

out_link:  Output: exit link with highest posterior probability

Posterior probability of the best link exiting this node. Log is
expressed in the log-base used in the decoder. To convert to linear
floating-point, use logmath_exp(ps_lattice_get_logmath(), pprob). ";

%feature("docstring")  ps_latlink_iter_next "

Get next link from a lattice link iterator.

Parameters:
-----------

itor:  Iterator.

Updated iterator, or NULL if finished. ";

%feature("docstring")  ps_latlink_iter_free "

Stop iterating over links.

Parameters:
-----------

itor:  Link iterator. ";

%feature("docstring")  ps_latlink_iter_link "

Get link from iterator. ";

%feature("docstring")  ps_latlink_times "

Get start and end times from a lattice link.

these are inclusive - i.e. the last frame of this word is ef, not
ef-1.

Parameters:
-----------

link:  Link inquired about.

out_sf:  Output: (optional) start frame of this link.

End frame of this link. ";

%feature("docstring")  ps_latlink_nodes "

Get destination and source nodes from a lattice link.

Parameters:
-----------

link:  Link inquired about

out_src:  Output: (optional) source node.

destination node ";

%feature("docstring")  Lattice::ps_latlink_word "

Get word string from a lattice link.

Parameters:
-----------

dag:  Lattice to which node belongs.

link:  Link inquired about

Word string for this link (possibly a pronunciation variant). ";

%feature("docstring")  Lattice::ps_latlink_baseword "

Get base word string from a lattice link.

Parameters:
-----------

dag:  Lattice to which node belongs.

link:  Link inquired about

Base word string for this link ";

%feature("docstring")  ps_latlink_pred "

Get predecessor link in best path.

Parameters:
-----------

link:  Link inquired about

Best previous link from bestpath search, if any. Otherwise NULL ";

%feature("docstring")  Lattice::ps_latlink_prob "

Get acoustic score and posterior probability from a lattice link.

Parameters:
-----------

dag:  Lattice to which node belongs.

link:  Link inquired about

out_ascr:  Output: (optional) acoustic score.

Posterior probability for this link. Log is expressed in the log-base
used in the decoder. To convert to linear floating-point, use
logmath_exp(ps_lattice_get_logmath(), pprob). ";

%feature("docstring")  Lattice::link "

Create a directed link between \"from\" and \"to\" nodes, but if a
link already exists, choose one with the best link_scr. ";

%feature("docstring")  Lattice::traverse_edges "

Start a forward traversal of edges in a word graph.

A keen eye will notice an inconsistency in this API versus other types
of iterators in PocketSphinx. The reason for this is that the
traversal algorithm is much more efficient when it is able to modify
the lattice structure. Therefore, to avoid giving the impression that
multiple traversals are possible at once, no separate iterator
structure is provided.

Parameters:
-----------

dag:  Lattice to be traversed.

start:  Start node (source) of traversal.

end:  End node (goal) of traversal.

First link in traversal. ";

%feature("docstring")  Lattice::traverse_next "

Get the next link in forward traversal.

Parameters:
-----------

dag:  Lattice to be traversed.

end:  End node (goal) of traversal.

Next link in traversal. ";

%feature("docstring")  Lattice::reverse_edges "

Start a reverse traversal of edges in a word graph.

See ps_lattice_traverse_edges() for why this API is the way it is.

Parameters:
-----------

dag:  Lattice to be traversed.

start:  Start node (goal) of traversal.

end:  End node (source) of traversal.

First link in traversal. ";

%feature("docstring")  Lattice::reverse_next "

Get the next link in reverse traversal.

Parameters:
-----------

dag:  Lattice to be traversed.

start:  Start node (goal) of traversal.

Next link in traversal. ";

%feature("docstring")  Lattice::bestpath "

Do N-Gram based best-path search on a word graph.

This function calculates both the best path as well as the forward
probability used in confidence estimation.

Final link in best path, NULL on error. ";

%feature("docstring")  Lattice::posterior "

Calculate link posterior probabilities on a word graph.

This function assumes that bestpath search has already been done.

Posterior probability of the utterance as a whole. ";

%feature("docstring")  Lattice::posterior_prune "

Prune all links (and associated nodes) below a certain posterior
probability.

This function assumes that ps_lattice_posterior() has already been
called.

Parameters:
-----------

beam:  Minimum posterior probability for links. This is expressed in
the log- base used in the decoder. To convert from linear floating-
point, use logmath_log(ps_lattice_get_logmath(), prob).

number of arcs removed. ";

%feature("docstring")  Lattice::n_frames "

Get the number of frames in the lattice.

Parameters:
-----------

dag:  The lattice in question.

Number of frames in this lattice. ";


// File: ps__mllr_8h.xml
%feature("docstring")  ps_mllr_read "

Read a speaker-adaptive linear transform from a file. ";

%feature("docstring")  ps_mllr_retain "

Retain a pointer to a linear transform. ";

%feature("docstring")  ps_mllr_free "

Release a pointer to a linear transform. ";


// File: ps__search_8h.xml
%feature("docstring")  Decoder::set_search "

Actives search with the provided name.

Activates search with the provided name. The search must be added
before using either ps_set_fsg(), ps_set_lm() or ps_set_kws().

0 on success, -1 on failure ";

%feature("docstring")  Decoder::get_search "

Returns name of curent search in decoder.

See:   ps_set_search ";

%feature("docstring")  Decoder::unset_search "

Unsets the search and releases related resources.

Unsets the search previously added with using either ps_set_fsg(),
ps_set_lm() or ps_set_kws().

See:   ps_set_fsg

See:   ps_set_lm

See:   ps_set_kws ";

%feature("docstring")  Decoder::search_iter "

Returns iterator over current searches.

See:   ps_set_search ";

%feature("docstring")  ps_search_iter_next "

Updates search iterator to point to the next position.

This function automatically frees the iterator object upon reaching
the final entry. See:   ps_set_search ";

%feature("docstring")  ps_search_iter_val "

Retrieves the name of the search the iterator points to.

Updates search iterator to point to the next position.

See:   ps_set_search  This function automatically frees the iterator
object upon reaching the final entry. See:   ps_set_search ";

%feature("docstring")  ps_search_iter_free "

Delete an unfinished search iterator.

See:   ps_set_search ";

%feature("docstring")  Decoder::get_lm "

Get the language model set object for this decoder.

If N-Gram decoding is not enabled, this will return NULL. You will
need to enable it using ps_set_lmset().

The language model set object for this decoder. The decoder retains
ownership of this pointer, so you should not attempt to free it
manually. Use ngram_model_retain() if you wish to reuse it elsewhere.
";

%feature("docstring")  Decoder::set_lm "

Adds new search based on N-gram language model.

Associates N-gram search with the provided name. The search can be
activated using ps_set_search().

See:   ps_set_search. ";

%feature("docstring")  Decoder::set_lm_file "

Adds new search based on N-gram language model.

Convenient method to load N-gram model and create a search.

See:   ps_set_lm ";

%feature("docstring")  Decoder::get_fsg "

Get the finite-state grammar set object for this decoder.

If FSG decoding is not enabled, this returns NULL. Call
ps_set_fsgset() to enable it.

The current FSG set object for this decoder, or NULL if none is
available. ";

%feature("docstring")  Decoder::set_fsg "

Adds new search based on finite state grammar.

Associates FSG search with the provided name. The search can be
activated using ps_set_search().

See:   ps_set_search ";

%feature("docstring")  Decoder::set_jsgf_file "

Adds new search using JSGF model.

Convenient method to load JSGF model and create a search.

See:   ps_set_fsg ";

%feature("docstring")  Decoder::set_jsgf_string "

Adds new search using JSGF model.

Convenience method to parse JSGF model from string and create a
search.

See:   ps_set_fsg ";

%feature("docstring")  Decoder::get_kws "

Get the current Key phrase to spot.

If KWS is not enabled, this returns NULL. Call ps_update_kws() to
enable it.

The current keyphrase to spot ";

%feature("docstring")  Decoder::set_kws "

Adds keywords from a file to spotting.

Associates KWS search with the provided name. The search can be
activated using ps_set_search().

See:   ps_set_search ";

%feature("docstring")  Decoder::set_keyphrase "

Adds new keyword to spot.

Associates KWS search with the provided name. The search can be
activated using ps_set_search().

See:   ps_set_search ";

%feature("docstring")  Decoder::set_allphone "

Adds new search based on phone N-gram language model.

Associates N-gram search with the provided name. The search can be
activated using ps_set_search().

See:   ps_set_search. ";

%feature("docstring")  Decoder::set_allphone_file "

Adds new search based on phone N-gram language model.

Convenient method to load N-gram model and create a search.

See:   ps_set_allphone ";


// File: ps__alignment_8c.xml
%feature("docstring")  ps_alignment_init "

Create a new, empty alignment. ";

%feature("docstring")  ps_alignment_free "

Release an alignment. ";

%feature("docstring")  vector_grow_one "";

%feature("docstring")  ps_alignment_vector_grow_one "";

%feature("docstring")  ps_alignment_vector_empty "";

%feature("docstring")  ps_alignment_add_word "

Append a word. ";

%feature("docstring")  ps_alignment_populate "

Populate lower layers using available word information. ";

%feature("docstring")  ps_alignment_populate_ci "

Populate lower layers using context-independent phones. ";

%feature("docstring")  ps_alignment_propagate "

Propagate timing information up from state sequence. ";

%feature("docstring")  ps_alignment_n_words "

Number of words. ";

%feature("docstring")  ps_alignment_n_phones "

Number of phones. ";

%feature("docstring")  ps_alignment_n_states "

Number of states. ";

%feature("docstring")  ps_alignment_words "

Iterate over the alignment starting at the first word. ";

%feature("docstring")  ps_alignment_phones "

Iterate over the alignment starting at the first phone. ";

%feature("docstring")  ps_alignment_states "

Iterate over the alignment starting at the first state. ";

%feature("docstring")  ps_alignment_iter_get "

Get the alignment entry pointed to by an iterator. ";

%feature("docstring")  ps_alignment_iter_free "

Release an iterator before completing all iterations. ";

%feature("docstring")  ps_alignment_iter_goto "

Move alignment iterator to given index. ";

%feature("docstring")  ps_alignment_iter_next "

Move an alignment iterator forward. ";

%feature("docstring")  ps_alignment_iter_prev "

Move an alignment iterator back. ";

%feature("docstring")  ps_alignment_iter_up "

Get a new iterator starting at the parent of the current node. ";

%feature("docstring")  ps_alignment_iter_down "

Get a new iterator starting at the first child of the current node. ";


// File: ps__alignment_8h.xml
%feature("docstring")  ps_alignment_init "

Create a new, empty alignment. ";

%feature("docstring")  ps_alignment_free "

Release an alignment. ";

%feature("docstring")  ps_alignment_add_word "

Append a word. ";

%feature("docstring")  ps_alignment_populate "

Populate lower layers using available word information. ";

%feature("docstring")  ps_alignment_populate_ci "

Populate lower layers using context-independent phones. ";

%feature("docstring")  ps_alignment_propagate "

Propagate timing information up from state sequence. ";

%feature("docstring")  ps_alignment_n_words "

Number of words. ";

%feature("docstring")  ps_alignment_n_phones "

Number of phones. ";

%feature("docstring")  ps_alignment_n_states "

Number of states. ";

%feature("docstring")  ps_alignment_words "

Iterate over the alignment starting at the first word. ";

%feature("docstring")  ps_alignment_phones "

Iterate over the alignment starting at the first phone. ";

%feature("docstring")  ps_alignment_states "

Iterate over the alignment starting at the first state. ";

%feature("docstring")  ps_alignment_iter_get "

Get the alignment entry pointed to by an iterator. ";

%feature("docstring")  ps_alignment_iter_goto "

Move alignment iterator to given index. ";

%feature("docstring")  ps_alignment_iter_next "

Move an alignment iterator forward. ";

%feature("docstring")  ps_alignment_iter_prev "

Move an alignment iterator back. ";

%feature("docstring")  ps_alignment_iter_up "

Get a new iterator starting at the parent of the current node. ";

%feature("docstring")  ps_alignment_iter_down "

Get a new iterator starting at the first child of the current node. ";

%feature("docstring")  ps_alignment_iter_free "

Release an iterator before completing all iterations. ";


// File: ps__lattice_8c.xml
%feature("docstring")  Lattice::link "

Create a directed link between \"from\" and \"to\" nodes, but if a
link already exists, choose one with the best link_scr. ";

%feature("docstring")  Lattice::penalize_fillers "

Insert penalty for fillers. ";

%feature("docstring")  Lattice::delete_node "";

%feature("docstring")  Lattice::remove_dangling_links "";

%feature("docstring")  Lattice::delete_unreachable "

Remove nodes marked as unreachable. ";

%feature("docstring")  Lattice::write "

Write a lattice to disk.

0 for success, <0 on failure. ";

%feature("docstring")  Lattice::write_htk "

Write a lattice to disk in HTK format.

0 for success, <0 on failure. ";

%feature("docstring")  dag_param_read "";

%feature("docstring")  dag_mark_reachable "";

%feature("docstring")  ps_lattice_read "

Read a lattice from a file on disk.

Parameters:
-----------

ps:  Decoder to use for processing this lattice, or NULL.

file:  Path to lattice file.

Newly created lattice, or NULL for failure. ";

%feature("docstring")  Lattice::n_frames "

Get the number of frames in the lattice.

Parameters:
-----------

dag:  The lattice in question.

Number of frames in this lattice. ";

%feature("docstring")  ps_lattice_init_search "

Construct an empty word graph with reference to a search structure. ";

%feature("docstring")  Lattice::retain "

Retain a lattice.

This function retains ownership of a lattice for the caller,
preventing it from being freed automatically. You must call
ps_lattice_free() to free it after having called this function.

pointer to the retained lattice. ";

%feature("docstring")  Lattice::free "

Free a lattice.

new reference count (0 if dag was freed) ";

%feature("docstring")  Lattice::get_logmath "

Get the log-math computation object for this lattice.

The log-math object for this lattice. The lattice retains ownership of
this pointer, so you should not attempt to free it manually. Use
logmath_retain() if you wish to reuse it elsewhere. ";

%feature("docstring")  Lattice::ps_latnode_iter "

Start iterating over nodes in the lattice.

No particular order of traversal is guaranteed, and you should not
depend on this.

Parameters:
-----------

dag:  Lattice to iterate over.

Iterator over lattice nodes. ";

%feature("docstring")  ps_latnode_iter_next "

Move to next node in iteration.

Parameters:
-----------

itor:  Node iterator.

Updated node iterator, or NULL if finished ";

%feature("docstring")  ps_latnode_iter_free "

Stop iterating over nodes.

Parameters:
-----------

itor:  Node iterator. ";

%feature("docstring")  ps_latnode_iter_node "

Get node from iterator. ";

%feature("docstring")  ps_latnode_times "

Get start and end time range for a node.

Parameters:
-----------

node:  Node inquired about.

out_fef:  Output: End frame of first exit from this node.

out_lef:  Output: End frame of last exit from this node.

Start frame for all edges exiting this node. ";

%feature("docstring")  Lattice::ps_latnode_word "

Get word string for this node.

Parameters:
-----------

dag:  Lattice to which node belongs.

node:  Node inquired about.

Word string for this node (possibly a pronunciation variant). ";

%feature("docstring")  Lattice::ps_latnode_baseword "

Get base word string for this node.

Parameters:
-----------

dag:  Lattice to which node belongs.

node:  Node inquired about.

Base word string for this node. ";

%feature("docstring")  Lattice::ps_latnode_prob "

Get best posterior probability and associated acoustic score from a
lattice node.

Parameters:
-----------

dag:  Lattice to which node belongs.

node:  Node inquired about.

out_link:  Output: exit link with highest posterior probability

Posterior probability of the best link exiting this node. Log is
expressed in the log-base used in the decoder. To convert to linear
floating-point, use logmath_exp(ps_lattice_get_logmath(), pprob). ";

%feature("docstring")  ps_latnode_exits "

Iterate over exits from this node.

Parameters:
-----------

node:  Node inquired about.

Iterator over exit links from this node. ";

%feature("docstring")  ps_latnode_entries "

Iterate over entries to this node.

Parameters:
-----------

node:  Node inquired about.

Iterator over entry links to this node. ";

%feature("docstring")  ps_latlink_iter_next "

Get next link from a lattice link iterator.

Parameters:
-----------

itor:  Iterator.

Updated iterator, or NULL if finished. ";

%feature("docstring")  ps_latlink_iter_free "

Stop iterating over links.

Parameters:
-----------

itor:  Link iterator. ";

%feature("docstring")  ps_latlink_iter_link "

Get link from iterator. ";

%feature("docstring")  ps_latlink_times "

Get start and end times from a lattice link.

these are inclusive - i.e. the last frame of this word is ef, not
ef-1.

Parameters:
-----------

link:  Link inquired about.

out_sf:  Output: (optional) start frame of this link.

End frame of this link. ";

%feature("docstring")  ps_latlink_nodes "

Get destination and source nodes from a lattice link.

Parameters:
-----------

link:  Link inquired about

out_src:  Output: (optional) source node.

destination node ";

%feature("docstring")  Lattice::ps_latlink_word "

Get word string from a lattice link.

Parameters:
-----------

dag:  Lattice to which node belongs.

link:  Link inquired about

Word string for this link (possibly a pronunciation variant). ";

%feature("docstring")  Lattice::ps_latlink_baseword "

Get base word string from a lattice link.

Parameters:
-----------

dag:  Lattice to which node belongs.

link:  Link inquired about

Base word string for this link ";

%feature("docstring")  ps_latlink_pred "

Get predecessor link in best path.

Parameters:
-----------

link:  Link inquired about

Best previous link from bestpath search, if any. Otherwise NULL ";

%feature("docstring")  Lattice::ps_latlink_prob "

Get acoustic score and posterior probability from a lattice link.

Parameters:
-----------

dag:  Lattice to which node belongs.

link:  Link inquired about

out_ascr:  Output: (optional) acoustic score.

Posterior probability for this link. Log is expressed in the log-base
used in the decoder. To convert to linear floating-point, use
logmath_exp(ps_lattice_get_logmath(), pprob). ";

%feature("docstring")  Lattice::hyp "

Get hypothesis string after bestpath search. ";

%feature("docstring")  Segment::ps_lattice_compute_lscr "";

%feature("docstring")  Segment::ps_lattice_link2itor "";

%feature("docstring")  Segment::ps_lattice_seg_free "";

%feature("docstring")  Segment::ps_lattice_seg_next "";

%feature("docstring")  Lattice::seg_iter "

Get hypothesis segmentation iterator after bestpath search. ";

%feature("docstring")  Lattice::latlink_list_new "

Create a new lattice link element. ";

%feature("docstring")  Lattice::pushq "

Add an edge to the traversal queue. ";

%feature("docstring")  Lattice::popq "

Remove an edge from the traversal queue. ";

%feature("docstring")  Lattice::delq "

Clear and reset the traversal queue. ";

%feature("docstring")  Lattice::traverse_edges "

Start a forward traversal of edges in a word graph.

A keen eye will notice an inconsistency in this API versus other types
of iterators in PocketSphinx. The reason for this is that the
traversal algorithm is much more efficient when it is able to modify
the lattice structure. Therefore, to avoid giving the impression that
multiple traversals are possible at once, no separate iterator
structure is provided.

Parameters:
-----------

dag:  Lattice to be traversed.

start:  Start node (source) of traversal.

end:  End node (goal) of traversal.

First link in traversal. ";

%feature("docstring")  Lattice::traverse_next "

Get the next link in forward traversal.

Parameters:
-----------

dag:  Lattice to be traversed.

end:  End node (goal) of traversal.

Next link in traversal. ";

%feature("docstring")  Lattice::reverse_edges "

Start a reverse traversal of edges in a word graph.

See ps_lattice_traverse_edges() for why this API is the way it is.

Parameters:
-----------

dag:  Lattice to be traversed.

start:  Start node (goal) of traversal.

end:  End node (source) of traversal.

First link in traversal. ";

%feature("docstring")  Lattice::reverse_next "

Get the next link in reverse traversal.

Parameters:
-----------

dag:  Lattice to be traversed.

start:  Start node (goal) of traversal.

Next link in traversal. ";

%feature("docstring")  Lattice::bestpath "

Do N-Gram based best-path search on a word graph.

This function calculates both the best path as well as the forward
probability used in confidence estimation.

Final link in best path, NULL on error. ";

%feature("docstring")  Lattice::joint "";

%feature("docstring")  Lattice::posterior "

Calculate link posterior probabilities on a word graph.

This function assumes that bestpath search has already been done.

Posterior probability of the utterance as a whole. ";

%feature("docstring")  Lattice::posterior_prune "

Prune all links (and associated nodes) below a certain posterior
probability.

This function assumes that ps_lattice_posterior() has already been
called.

Parameters:
-----------

beam:  Minimum posterior probability for links. This is expressed in
the log- base used in the decoder. To convert from linear floating-
point, use logmath_log(ps_lattice_get_logmath(), prob).

number of arcs removed. ";

%feature("docstring")  best_rem_score "";

%feature("docstring")  path_insert "";

%feature("docstring")  path_extend "";

%feature("docstring")  Lattice::ps_astar_start "

Begin N-Gram based A* search on a word graph.

Parameters:
-----------

sf:  Starting frame for N-best search.

ef:  Ending frame for N-best search, or -1 for last frame.

w1:  First context word, or -1 for none.

w2:  Second context word, or -1 for none.

0 for success, <0 on error. ";

%feature("docstring")  ps_astar_next "

Find next best hypothesis of A* on a word graph.

a complete path, or NULL if no more hypotheses exist. ";

%feature("docstring")  ps_astar_hyp "

Get hypothesis string from A* search. ";

%feature("docstring")  ps_astar_node2itor "";

%feature("docstring")  Segment::ps_astar_seg_free "";

%feature("docstring")  Segment::ps_astar_seg_next "";

%feature("docstring")  ps_astar_seg_iter "

Get hypothesis segmentation from A* search. ";

%feature("docstring")  ps_astar_finish "

Finish N-best search, releasing resources associated with it. ";


// File: ps__lattice__internal_8h.xml
%feature("docstring")  ps_lattice_init_search "

Construct an empty word graph with reference to a search structure. ";

%feature("docstring")  Lattice::penalize_fillers "

Insert penalty for fillers. ";

%feature("docstring")  Lattice::delete_unreachable "

Remove nodes marked as unreachable. ";

%feature("docstring")  Lattice::pushq "

Add an edge to the traversal queue. ";

%feature("docstring")  Lattice::popq "

Remove an edge from the traversal queue. ";

%feature("docstring")  Lattice::delq "

Clear and reset the traversal queue. ";

%feature("docstring")  Lattice::latlink_list_new "

Create a new lattice link element. ";

%feature("docstring")  Lattice::hyp "

Get hypothesis string after bestpath search. ";

%feature("docstring")  Lattice::seg_iter "

Get hypothesis segmentation iterator after bestpath search. ";

%feature("docstring")  Lattice::ps_astar_start "

Begin N-Gram based A* search on a word graph.

Parameters:
-----------

sf:  Starting frame for N-best search.

ef:  Ending frame for N-best search, or -1 for last frame.

w1:  First context word, or -1 for none.

w2:  Second context word, or -1 for none.

0 for success, <0 on error. ";

%feature("docstring")  ps_astar_next "

Find next best hypothesis of A* on a word graph.

a complete path, or NULL if no more hypotheses exist. ";

%feature("docstring")  ps_astar_finish "

Finish N-best search, releasing resources associated with it. ";

%feature("docstring")  ps_astar_hyp "

Get hypothesis string from A* search. ";

%feature("docstring")  ps_astar_seg_iter "

Get hypothesis segmentation from A* search. ";


// File: ps__mllr_8c.xml
%feature("docstring")  ps_mllr_read "

Read a speaker-adaptive linear transform from a file. ";

%feature("docstring")  ps_mllr_retain "

Retain a pointer to a linear transform. ";

%feature("docstring")  ps_mllr_free "

Release a pointer to a linear transform. ";

