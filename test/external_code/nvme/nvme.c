/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/nvme_spec.h"
#include "spdk/log.h"
#include "spdk/stdinc.h"
#include "nvme.h"

struct nvme_ctrlr {
	/* Underlying PCI device */
	struct spdk_pci_device			*pci_device;
	/* Pointer to the MMIO register space */
	volatile struct spdk_nvme_registers	*regs;
	TAILQ_ENTRY(nvme_ctrlr)			tailq;
};

static struct spdk_pci_id nvme_pci_driver_id[] = {
	{
		.class_id = SPDK_PCI_CLASS_NVME,
		.vendor_id = SPDK_PCI_ANY_ID,
		.device_id = SPDK_PCI_ANY_ID,
		.subvendor_id = SPDK_PCI_ANY_ID,
		.subdevice_id = SPDK_PCI_ANY_ID,
	},
	{ .vendor_id = 0, /* sentinel */ },
};

SPDK_PCI_DRIVER_REGISTER(nvme_external, nvme_pci_driver_id, SPDK_PCI_DRIVER_NEED_MAPPING);
static TAILQ_HEAD(, nvme_ctrlr) g_nvme_ctrlrs = TAILQ_HEAD_INITIALIZER(g_nvme_ctrlrs);

static struct nvme_ctrlr *
find_ctrlr_by_addr(struct spdk_pci_addr *addr)
{
	struct spdk_pci_addr ctrlr_addr;
	struct nvme_ctrlr *ctrlr;

	TAILQ_FOREACH(ctrlr, &g_nvme_ctrlrs, tailq) {
		ctrlr_addr = spdk_pci_device_get_addr(ctrlr->pci_device);
		if (spdk_pci_addr_compare(addr, &ctrlr_addr) == 0) {
			return ctrlr;
		}
	}

	return NULL;
}

static int
nvme_ctrlr_allocate_bars(struct nvme_ctrlr *ctrlr)
{
	uint64_t phys_addr, size;
	void *addr;
	int rc;

	rc = spdk_pci_device_map_bar(ctrlr->pci_device, 0, &addr, &phys_addr, &size);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to map BAR 0\n");
		return rc;
	}

	ctrlr->regs = (volatile struct spdk_nvme_registers *)addr;
	return 0;
}

static int
pcie_nvme_enum_cb(void *ctx, struct spdk_pci_device *pci_dev)
{
	struct nvme_ctrlr *ctrlr;
	TAILQ_HEAD(, nvme_ctrlr) *ctrlrs = ctx;
	uint16_t cmd_reg;
	char addr[64] = {};

	spdk_pci_addr_fmt(addr, sizeof(addr), &pci_dev->addr);

	ctrlr = calloc(1, sizeof(*ctrlr));
	if (!ctrlr) {
		SPDK_ERRLOG("Failed to allocate NVMe controller: %s\n", addr);
		return -1;
	}

	ctrlr->pci_device = pci_dev;
	if (spdk_pci_device_claim(pci_dev)) {
		SPDK_ERRLOG("Failed to claim PCI device: %s\n", addr);
		free(ctrlr);
		return -1;
	}

	if (nvme_ctrlr_allocate_bars(ctrlr)) {
		SPDK_ERRLOG("Failed to allocate BARs for NVMe controller: %s\n", addr);
		spdk_pci_device_unclaim(pci_dev);
		free(ctrlr);
		return -1;
	}

	/* Enable PCI busmaster and disable INTx */
	spdk_pci_device_cfg_read16(pci_dev, &cmd_reg, 4);
	cmd_reg |= 0x404;
	spdk_pci_device_cfg_write16(pci_dev, cmd_reg, 4);

	TAILQ_INSERT_TAIL(ctrlrs, ctrlr, tailq);

	return 0;
}

static void
nvme_ctrlr_free(struct nvme_ctrlr *ctrlr)
{
	spdk_pci_device_unclaim(ctrlr->pci_device);
	spdk_pci_device_detach(ctrlr->pci_device);
	free(ctrlr);
}

static int
probe_internal(struct spdk_pci_addr *addr, nvme_attach_cb attach_cb, void *cb_ctx)
{
	struct nvme_ctrlr *ctrlr, *tmp;
	TAILQ_HEAD(, nvme_ctrlr) ctrlrs = TAILQ_HEAD_INITIALIZER(ctrlrs);
	int rc;

	if (addr == NULL) {
		rc = spdk_pci_enumerate(spdk_pci_get_driver("nvme_external"),
					pcie_nvme_enum_cb, &ctrlrs);
	} else {
		rc = spdk_pci_device_attach(spdk_pci_get_driver("nvme_external"),
					    pcie_nvme_enum_cb, &ctrlrs, addr);
	}

	if (rc != 0) {
		SPDK_ERRLOG("Failed to enumerate PCI devices\n");
		while (!TAILQ_EMPTY(&ctrlrs)) {
			ctrlr = TAILQ_FIRST(&ctrlrs);
			TAILQ_REMOVE(&ctrlrs, ctrlr, tailq);
			nvme_ctrlr_free(ctrlr);
		}

		return rc;
	}

	TAILQ_FOREACH_SAFE(ctrlr, &ctrlrs, tailq, tmp) {
		TAILQ_REMOVE(&ctrlrs, ctrlr, tailq);
		TAILQ_INSERT_TAIL(&g_nvme_ctrlrs, ctrlr, tailq);

		if (attach_cb != NULL) {
			attach_cb(cb_ctx, &ctrlr->pci_device->addr, ctrlr);
		}
	}

	return 0;
}

int
nvme_probe(nvme_attach_cb attach_cb, void *cb_ctx)
{
	return probe_internal(NULL, attach_cb, cb_ctx);
}

struct nvme_ctrlr *
nvme_connect(struct spdk_pci_addr *addr)
{
	int rc;

	rc = probe_internal(addr, NULL, NULL);
	if (rc != 0) {
		return NULL;
	}

	return find_ctrlr_by_addr(addr);
}

void
nvme_detach(struct nvme_ctrlr *ctrlr)
{
	TAILQ_REMOVE(&g_nvme_ctrlrs, ctrlr, tailq);
	nvme_ctrlr_free(ctrlr);
}

SPDK_LOG_REGISTER_COMPONENT(nvme_external)
