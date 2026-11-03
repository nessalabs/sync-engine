---- MODULE ArtifactPublish ----
EXTENDS Naturals
VARIABLES offset, validHash, version, published, savedOffset, savedHash, savedVersion
vars == <<offset,validHash,version,published,savedOffset,savedHash,savedVersion>>
Init == /\ offset = 0 /\ validHash \in BOOLEAN /\ version = 1 /\ published = FALSE /\ savedOffset = 0 /\ savedHash = FALSE /\ savedVersion = 0
Chunk == /\ offset < 2 /\ ~published /\ offset' = offset + 1 /\ UNCHANGED <<validHash,version,published,savedOffset,savedHash,savedVersion>>
Replace == /\ version = 1 /\ version' = 2 /\ UNCHANGED <<offset,validHash,published,savedOffset,savedHash,savedVersion>>
Publish == /\ ~published /\ offset = 2 /\ validHash /\ version = 1
           /\ published' = TRUE /\ savedOffset' = offset /\ savedHash' = validHash /\ savedVersion' = version
           /\ UNCHANGED <<offset,validHash,version>>
Next == Chunk \/ Replace \/ Publish
CompleteBeforeVisible == published => savedOffset = 2
HashBeforeVisible == published => savedHash
ExactManifest == published => savedVersion = 1
====
