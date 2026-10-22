-- Preserve every block candidate. Only a future explicit reconciliation stage
-- may introduce canonical states other than unknown.
ALTER TABLE loader.blocks
 ADD COLUMN canonical_status text NOT NULL DEFAULT 'unknown' CHECK (canonical_status='unknown'),
 ADD COLUMN height_conflict boolean NOT NULL DEFAULT false,
 ADD COLUMN parent_link text NOT NULL DEFAULT 'unknown' CHECK (parent_link IN ('unknown','linked','mismatch'));

-- Initialize observations for data loaded before this forward migration.
-- The owning migrator applies this within the existing migration transaction.
UPDATE loader.blocks b SET
 height_conflict=EXISTS (
  SELECT 1 FROM loader.blocks other
  WHERE other.chain_id=b.chain_id AND other.block_number=b.block_number AND other.block_hash<>b.block_hash),
 parent_link=CASE
  WHEN b.block_number=0 THEN 'unknown'
  WHEN EXISTS (SELECT 1 FROM loader.blocks p WHERE p.chain_id=b.chain_id AND p.block_number=b.block_number-1 AND p.block_hash=b.parent_hash) THEN 'linked'
  WHEN EXISTS (SELECT 1 FROM loader.blocks p WHERE p.chain_id=b.chain_id AND p.block_number=b.block_number-1) THEN 'mismatch'
  ELSE 'unknown' END;

CREATE INDEX blocks_observed_conflicts ON loader.blocks(chain_id,block_number,block_hash)
 WHERE height_conflict OR parent_link='mismatch';
GRANT UPDATE (height_conflict,parent_link) ON loader.blocks TO loader_runtime;
