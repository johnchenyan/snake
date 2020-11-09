package sealing

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/lib/snakestar"
	"golang.org/x/xerrors"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/filecoin-project/go-state-types/abi"
)

func (m *Sealing) pledgeSector(ctx context.Context, sectorID abi.SectorID, existingPieceSizes []abi.UnpaddedPieceSize, sizes ...abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
	if len(sizes) == 0 {
		return nil, nil
	}

	log.Infof("Pledge %d, contains %+v", sectorID, existingPieceSizes)

	out := make([]abi.PieceInfo, len(sizes))
	for i, size := range sizes {
		ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, NewNullReader(size))
		if err != nil {
			return nil, xerrors.Errorf("add piece: %w", err)
		}

		existingPieceSizes = append(existingPieceSizes, size)

		out[i] = ppi
	}

	return out, nil
}

func (m *Sealing) PledgeSector() error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() > cfg.MaxSealingSectors {
			return xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		size := abi.PaddedPieceSize(m.sealer.SectorSize()).Unpadded()

		/* snake begin */
		var (
			sid abi.SectorNumber
			err error
		)
		if snakestar.PledgeSector && snakestar.WindowPost {
			sid, err = m.sc.Next()
		} else {
			sid, err = m.nextSectorFromServer(context.TODO(), "http://"+snakestar.ServerAddress+"/snake/nextid")
		}
		/* snake end  */
		//sid, err := m.sc.Next() // snake del
		if err != nil {
			log.Errorf("get next sector id %+v", err)
			return
		}
		err = m.sealer.NewSector(ctx, m.minerSector(sid))
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		//pieces, err := m.pledgeSector(ctx, m.minerSector(sid), []abi.UnpaddedPieceSize{}, size) //sanke del
		pieces, err := m.pledgeSectorWithCache(ctx, m.minerSector(sid), []abi.UnpaddedPieceSize{}, size) //ipfsunion add
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		ps := make([]Piece, len(pieces))
		for idx := range ps {
			ps[idx] = Piece{
				Piece:    pieces[idx],
				DealInfo: nil,
			}
		}

		if err := m.newSectorCC(sid, ps); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}

/* snake begin */
func (m *Sealing) nextSectorFromServer(ctx context.Context, url string) (abi.SectorNumber, error) {
	if url == "" {
		return 0, xerrors.Errorf("invalid server address %s", url)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, xerrors.Errorf("request: %w", err)
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, xerrors.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, xerrors.Errorf("non-200 code: %d", resp.StatusCode)
	}

	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipfs-shell: warning! response (%d) read error: %s\n", resp.StatusCode, err)
	}
	//get
	str := strings.ReplaceAll(strings.ReplaceAll(string(out), "\"", ""), "\n", "")
	log.Infof("nextSectorFromServer next sector id %s")
	sn, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, err
	}

	return abi.SectorNumber(sn), nil
}

/* snake end */
