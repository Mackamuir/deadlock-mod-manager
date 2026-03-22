import { EntityNotFoundError } from "@deadlock-mods/common";
import { and, asc, desc, eq, inArray } from "@deadlock-mods/database";
import type { Database } from "../client";
import type { Mod, NewMod } from "../schema/mods";
import { mods } from "../schema/mods";

export interface FindAllOptions {
  sortBy?: "downloadCount" | "remoteAddedAt" | "remoteUpdatedAt";
  order?: "asc" | "desc";
  limit?: number;
}

const sortColumnMap: Record<
  NonNullable<FindAllOptions["sortBy"]>,
  typeof mods.downloadCount | typeof mods.remoteAddedAt | typeof mods.remoteUpdatedAt
> = {
  downloadCount: mods.downloadCount,
  remoteAddedAt: mods.remoteAddedAt,
  remoteUpdatedAt: mods.remoteUpdatedAt,
};

export class ModRepository {
  constructor(private readonly db: Database) {}

  async findAll(options?: FindAllOptions): Promise<Mod[]> {
    const column = sortColumnMap[options?.sortBy ?? "remoteUpdatedAt"];
    const orderFn = options?.order === "asc" ? asc : desc;

    const query = this.db
      .select()
      .from(mods)
      .where(eq(mods.isBlacklisted, false))
      .orderBy(orderFn(column));

    if (options?.limit) {
      return await query.limit(options.limit);
    }

    return await query;
  }

  async findById(id: string): Promise<Mod | null> {
    const result = await this.db
      .select()
      .from(mods)
      .where(eq(mods.id, id))
      .limit(1);
    return result.length > 0 ? result[0] : null;
  }

  async findByRemoteId(remoteId: string): Promise<Mod | null> {
    const result = await this.db
      .select()
      .from(mods)
      .where(and(eq(mods.remoteId, remoteId), eq(mods.isBlacklisted, false)))
      .limit(1);
    return result.length > 0 ? result[0] : null;
  }

  async findByRemoteIdIncludingBlacklisted(
    remoteId: string,
  ): Promise<Mod | null> {
    const result = await this.db
      .select()
      .from(mods)
      .where(eq(mods.remoteId, remoteId))
      .limit(1);
    return result.length > 0 ? result[0] : null;
  }

  async findByRemoteIds(remoteIds: string[]): Promise<Mod[]> {
    if (remoteIds.length === 0) {
      return [];
    }
    return await this.db
      .select()
      .from(mods)
      .where(
        and(inArray(mods.remoteId, remoteIds), eq(mods.isBlacklisted, false)),
      )
      .orderBy(desc(mods.remoteUpdatedAt));
  }

  async create(mod: NewMod): Promise<Mod> {
    const result = await this.db.insert(mods).values(mod).returning();
    return result[0];
  }

  async update(id: string, mod: Partial<NewMod>): Promise<Mod> {
    const result = await this.db
      .update(mods)
      .set({ ...mod, updatedAt: new Date() })
      .where(eq(mods.id, id))
      .returning();
    if (result.length === 0) {
      throw new EntityNotFoundError("mod", id);
    }
    return result[0];
  }

  async updateByRemoteId(remoteId: string, mod: Partial<NewMod>): Promise<Mod> {
    const result = await this.db
      .update(mods)
      .set({ ...mod, updatedAt: new Date() })
      .where(eq(mods.remoteId, remoteId))
      .returning();
    if (result.length === 0) {
      throw new EntityNotFoundError("mod", remoteId);
    }
    return result[0];
  }

  async upsertByRemoteId(mod: NewMod): Promise<Mod> {
    const {
      id: _id,
      createdAt: _createdAt,
      isBlacklisted: _isBlacklisted,
      blacklistReason: _blacklistReason,
      blacklistedAt: _blacklistedAt,
      blacklistedBy: _blacklistedBy,
      ...updateableFields
    } = mod;

    const [result] = await this.db
      .insert(mods)
      .values(mod)
      .onConflictDoUpdate({
        target: mods.remoteId,
        set: {
          ...updateableFields,
          updatedAt: new Date(),
        },
      })
      .returning();

    return result;
  }

  async delete(id: string): Promise<void> {
    await this.db.delete(mods).where(eq(mods.id, id));
  }

  async deleteByRemoteId(remoteId: string): Promise<void> {
    await this.db.delete(mods).where(eq(mods.remoteId, remoteId));
  }

  async exists(id: string): Promise<boolean> {
    const result = await this.db
      .select({ id: mods.id })
      .from(mods)
      .where(eq(mods.id, id))
      .limit(1);
    return result.length > 0;
  }

  async existsByRemoteId(remoteId: string): Promise<boolean> {
    const result = await this.db
      .select({ id: mods.id })
      .from(mods)
      .where(eq(mods.remoteId, remoteId))
      .limit(1);
    return result.length > 0;
  }

  async blacklistMod(
    remoteId: string,
    reason: string,
    blacklistedBy: string,
  ): Promise<Mod> {
    const result = await this.db
      .update(mods)
      .set({
        isBlacklisted: true,
        blacklistReason: reason,
        blacklistedAt: new Date(),
        blacklistedBy,
        updatedAt: new Date(),
      })
      .where(eq(mods.remoteId, remoteId))
      .returning();
    if (result.length === 0) {
      throw new EntityNotFoundError("mod", remoteId);
    }
    return result[0];
  }

  async unblacklistMod(remoteId: string): Promise<Mod> {
    const result = await this.db
      .update(mods)
      .set({
        isBlacklisted: false,
        blacklistReason: null,
        blacklistedAt: null,
        blacklistedBy: null,
        updatedAt: new Date(),
      })
      .where(eq(mods.remoteId, remoteId))
      .returning();
    if (result.length === 0) {
      throw new EntityNotFoundError("mod", remoteId);
    }
    return result[0];
  }
}
